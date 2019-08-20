[![Build Status](https://travis-ci.org/open-io/ansible-role-openio-mover-agent.svg?branch=master)](https://travis-ci.org/open-io/ansible-role-openio-mover-agent)
# Ansible role `mover_agent`

An Ansible role for OpenIO mover agent. Specifically, the responsibilities of this role are to:

- Install and configure

## Requirements

- Ansible 2.4+

## Role Variables

| Variable   | Default | Comments (type)  |
| :---       | :---    | :---             |
| `openio_mover_agent_gridinit_dir` | `"/etc/gridinit.d/{{ openio_mover_agent_namespace }}"` | Path to copy the gridinit conf |
| `openio_mover_agent_gridinit_prefix` | `""` | Maybe set it to {{ openio_ecd_namespace }}- for old gridinit's style |
| `openio_mover_agent_bind_port` | `6799` | Bind on port |
| `openio_mover_agent_location` | `...` | Conscience location |
| `openio_mover_agent_bind_interface` | `"{{ ansible_default_ipv4.alias }}"` | Bind on interface |
| `openio_mover_agent_bind_address` | `...` | Bind on address |
| `openio_mover_agent_namespace` | `"OPENIO"` | Namespace |
| `openio_mover_agent_provision_only` | `false` | Provision only without restarting services |
| `openio_blob_indexer_report_interval` | `5` | Secondes to display progression |
| `openio_mover_agent_serviceid` | `"0"` | ID in gridinit |
| `openio_mover_agent_slots` | `...` | Slots for mover agent |

## Dependencies

No dependencies.

## Example Playbook

```yaml
- hosts: all
  become: true
  vars:
    NS: OPENIO
  roles:
    - role: repo
      openio_repository_no_log: false
      openio_repository_products:
        sds:
          release: "18.04"
    - role: namespace
      openio_namespace_name: "{{ NS }}"
    - role: gridinit
      openio_gridinit_namespace: "{{ NS }}"
      openio_gridinit_per_ns: true
    - role: oio-blolb-indexer
      openio_mover_agent_namespace: "{{ NS }}"

```


```ini
[all]
node1 ansible_host=192.168.1.173
```

## Contributing

Issues, feature requests, ideas are appreciated and can be posted in the Issues section.

Pull requests are also very welcome.
The best way to submit a PR is by first creating a fork of this Github project, then creating a topic branch for the suggested change and pushing that branch to your own fork.
Github can then easily create a PR based on that branch.

## License

GNU AFFERO GENERAL PUBLIC LICENSE, Version 3

## Contributors

- [Cedric DELGEHIER](https://github.com/cdelgehier) (maintainer)
