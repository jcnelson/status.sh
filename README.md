# status.sh

Self-contained single-file Stacks chain explorer, written in 100% `bash`.

It should work on any modern Linux distribution (tested on Alpine 3.12, Ubuntu
20.04, and Amazon Linux 2).  Some specific requrements:

* Bash 5.x (might work with 4.3)
* OpenSSL 1.1.x
* `blockstack-cli` from https://github.com/blockstack/stacks-blockchain

Endpoints return data as HTML pages or JSON.  Most HTML endpoints have a JSON equivalent;
read the code for details.  The program is also capable of running as a one-shot command for
generating JSON reports.

Pages are best viewed with `lynx`.
