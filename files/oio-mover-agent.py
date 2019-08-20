#!/usr/bin/env python
# Copyright (C) 2019 OpenIO SAS, as part of OpenIO SDS
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3.0 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library.

from __future__ import print_function

import uuid
import json
import cgi
import os
import time
import argparse
import multiprocessing as mp
from traceback import format_exc

from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer

from oio.blob.mover import BlobMoverWorker
from oio.conscience.client import ConscienceClient
from oio.cli import make_logger_args_parser, get_logger_from_args

legacy_meta2 = False
try:
    from oio.directory.meta2 import Meta2Database
except ImportError:
    from oio.directory.meta1 import Meta1RefMapping
    legacy_meta2 = True


class BlobStatsLogger(object):
    """
    Log interceptor to parse logs coming from blob mover
    and turn them into stats
    """

    def __init__(self, logger, success, fail, size):
        self.log = logger
        self.success = success
        self.fail = fail
        self.size = size

    def info(self, msg, *args, **kwargs):
        self.log.debug(msg % args)
        if msg.startswith("moved"):
            self.success.value += 1

    def warn(self, msg, *args, **kwargs):
        self.log.debug(msg % args)

    def error(self, msg, *args, **kwargs):
        self.log.debug(msg % args)
        if msg.startswith("ERROR"):
            self.fail.value += 1

    def debug(self, msg, *args, **kwargs):
        self.log.debug(msg % args)


class OioMoverAgentClassManager(object):
    """
    Pluggable class manager
    """

    def __init__(self, ctx=dict()):
        self.blob_mover = BlobMoverWorker
        self.conscience = ConscienceClient
        if legacy_meta2:
            self.meta2_mover_legacy = Meta1RefMapping
        else:
            self.meta2_mover = Meta2Database

        for name, class_ in ctx.items():
            setattr(self, name, class_)


class OioMoverAgent(object):
    """
    Main class for the mover agent
    Responsible of running blob/meta2 movers via multiprocessing
    """
    jobs = dict()

    def __init__(self, args, **kwargs):
        # This allows for easily swappable classes to be used
        ctx = {}
        try:
            ctx = args.get("ctx", {})
        except Exception:
            pass
        self.cm = OioMoverAgentClassManager(ctx=ctx or kwargs.get("ctx", {}))
        self.host = args.host
        self.namespace = args.namespace
        self.client = self.cm.conscience({"namespace": args.namespace})
        self.log = get_logger_from_args(args)
        self.jobs = dict()

    def move_meta2_wrapper(self, src, base):
        """
            Wrapper for SDS 4.x/5.x compat
        """
        if legacy_meta2:
            mapping = self.cm.meta2_mover_legacy(self.namespace)
            moved = mapping.move(src, None, base, "meta2")
            return mapping.apply(moved, src_service=src)

        meta2 = self.cmd.meta2_mover({'namespace': self.namespace})
        moved = meta2.move(base, src)
        for res in moved:
            if res['err']:
                return False
        return True

    def move_meta2(self, src, bases, stats, end):
        """
        Job for meta2 mover
        """
        self.client.lock_score(dict(type="meta2", addr=src))
        for base in bases:
            try:
                if self.move_meta2_wrapper(src, base[0]):
                    stats.get("success").value += 1
                    stats.get("bytes").value += base[1]
                else:
                    stats.get("fail").value += 1
            except Exception:
                stats.get("fail").value += 1
        self.client.unlock_score(dict(type="meta2", addr=src))
        end.value = int(time.time())

    def move_blobs(self, src, config, stats, end):
        """
        Job for blob mover
        """
        self.client.lock_score(dict(type="rawx", addr=src))

        self.log.info("Starting blob mover on %s with config %s",
                      config['volume'],
                      ",".join(["%s=%s" % (k, v) for k, v in config.items()]))
        try:
            logger = BlobStatsLogger(self.log,
                                     stats.get("success"),
                                     stats.get("fail"),
                                     stats.get("bytes"))
            worker = self.cm.blob_mover(config, logger, config['volume'])
            worker.mover_pass()
        except Exception as e:
            self.log.error("Blob mover failed with error: %s" % format_exc(e))
        self.client.unlock_score(dict(type="rawx", addr=src))
        end.value = int(time.time())

    def check_running(self, vol):
        """
        Check if a mover job is already running on the specified volume
        """
        for _, job in self.jobs.items():
            if job.get("host") == self.host and job.get("volume") == vol\
                    and job.get("end").value == 0:
                return True

    def volume(self, type_, src):
        """
        Resolve the volume for the specified service
        """
        for svc in self.client.all_services(type_):
            tags = svc.get("tags", {})
            # Note: this
            if svc.get("addr") == src and self.host in tags.get("tag.loc"):
                return tags.get("tag.vol")
        return None

    def excluded(self, type_, exclude):
        """
        Resolve excluded rawx services from the exclude list
        """
        to_exclude = []
        services = self.client.all_services(type_)
        for svc in services:
            for excl in exclude:
                if excl in svc.get("tags", {}).get("tag.loc", []):
                    to_exclude.append(svc.get("addr"))
        return to_exclude

    def fetch_jobs(self):
        """
        Get the status/stats of all running jobs
        """
        res = []
        for id, job in self.jobs.items():
            data = dict(
                id=str(id),
                config=job["config"],
                stats=dict(),
                service=job["service"],
                volume=job["volume"],
                host=job["host"],
                type=job["type"],
                start=job["start"],
                end=job["end"].value
            )
            for k, v in job["stats"].items():
                if k == 'total' and v == 0:
                    data['stats'][k] = job['stats']['success'].value + \
                        job['stats']['fail'].value
                    continue
                try:
                    data['stats'][k] = v.value
                except Exception:
                    data['stats'][k] = v

            res.append(data)
        return res

    def run_job(self, type_, src, vol, opts):
        """
        Create a mover job on the specified service
        """
        id = str(uuid.uuid4())
        job = dict(
            type=type_,
            action="move",
            host=self.host,
            volume=vol,
            service=src,
            process=None,
            config=dict(),
            stats=dict(),
            start=int(time.time()),
            end=mp.Value('i')
        )

        if type_ == "meta2":
            bases = []
            for path, _, files in os.walk(vol):
                for file in files:
                    size = os.path.getsize(os.path.join(path, file))
                    if size < opts.get("minsize", 0):
                        continue
                    elif size > opts.get("maxsize", 1e32):
                        continue
                    bases.append([file.split('.1.meta2')[0], size])

            job['config'] = dict(src=src)
            for field in ("min_base_size", "max_base_size"):
                if opts.get(field):
                    job['config'][field] = opts.get(field)
            job['stats'] = dict(
                success=mp.Value('i'),
                fail=mp.Value('i'),
                bytes=mp.Value('i'),
                total=len(bases)
            )
            job["process"] = mp.Process(target=self.move_meta2, args=(
                src, bases, job["stats"], job["end"]))

        elif type_ == "rawx":
            excluded = self.excluded("rawx", opts.get('exclude', []))

            job['config'] = dict(
                volume=vol,
                namespace=self.namespace,
            )
            for field in [("bps", "bytes_per_second"),
                          ("cps", "chunks_per_second"),
                          ("concurrency", "concurrency"),
                          ("target", "usage_target"),
                          ("minsize", "min_chunk_size"),
                          ("maxsize", "max_chunk_size")]:
                if opts.get(field[0]):
                    job["config"][field[1]] = opts[field[0]]
            if excluded:
                job["config"]["excluded_rawx"] = ",".join(excluded)

            job['stats'] = dict(
                success=mp.Value('i'),
                fail=mp.Value('i'),
                bytes=mp.Value('i'),
                total=0
            )

            job["process"] = mp.Process(
                target=self.move_blobs,
                args=(
                    job['service'],
                    job['config'],
                    job["stats"],
                    job["end"],
                )
            )

        job['process'].start()
        self.jobs[id] = job
        return id


def make_handler(options):
    """
    Create a handler that serves mover agent requests
    """
    class OioMoverAgentHandler(BaseHTTPRequestHandler):
        """
        Mover agent handler
        """
        agent = OioMoverAgent(options)

        def __init__(self, *args, **kwargs):
            BaseHTTPRequestHandler.__init__(self, *args, **kwargs)

        def http(self, code, data=None, json_=None, err=None):
            self.send_response(code)
            self.end_headers()
            if data:
                self.wfile.write(data)
                return
            if err:
                json_ = dict(error=err)
            if json:
                self.wfile.write(json.dumps(json_))

        def do_GET(self):
            """
                Retrieve stats of a mover
            """
            if not self.path.startswith("/api/v1/jobs"):
                return self.http(404, err="Invalid URI")
            self.http(200, json_=self.agent.fetch_jobs())

        def do_POST(self):
            """
                Invokes a Meta2/Blob Mover locally on the node
            """

            if not self.path.startswith("/api/v1/jobs"):
                return self.http(404, err="Invalid URI")

            ct = self.headers.getheader('content-type')
            if not ct:
                return self.http(400, err="Invalid content-type, json expected")
            ctype, _ = cgi.parse_header(ct)

            if ctype != 'application/json':
                return self.http(400, err="Invalid content-type, json expected")

            length = int(self.headers.getheader('content-length'))
            req = json.loads(self.rfile.read(length))
            type_ = req.get('type')
            src = req.get('src')

            if type_ not in ("meta2", "rawx"):
                return self.http(400, err="Invalid service type")
            if not src:
                return self.http(400, err="Invalid service")

            vol = self.agent.volume(type_, src)
            if not vol:
                return self.http(400, err="Volume not found")

            if self.agent.check_running(vol):
                return self.http(
                    400,
                    err="A job is already running on the target volume"
                )

            id = self.agent.run_job(type_, src, vol, req)
            self.http(201, json_=dict(id=id))

    return OioMoverAgentHandler


def make_arg_parser():
    desc = """
        Stateless daemon providing an HTTP interface to run blob/meta2 movers
    """
    parser = argparse.ArgumentParser(description=desc,
                                     parents=[make_logger_args_parser()])
    parser.add_argument('--namespace',
                        metavar='<namespace>',
                        dest='namespace',
                        help="Namespace", required=True)
    parser.add_argument('--host',
                        metavar='<host>',
                        dest='host',
                        help="Hostname of current node",
                        required=True)
    parser.add_argument('--addr',
                        metavar='<addr>',
                        dest='addr',
                        help="IP:PORT to bind to", required=True)

    return parser


if __name__ == "__main__":
    args = make_arg_parser().parse_args()
    ip, port = args.addr.split(':')
    httpd = HTTPServer((ip, int(port)), make_handler(args))
    httpd.serve_forever()
