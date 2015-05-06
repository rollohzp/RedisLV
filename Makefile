# Top level makefile, the real shit is at src/Makefile

$(shell sh build.sh 1>&2)

default: all

.DEFAULT:
	cd src && $(MAKE) $@

install:
	cd src && $(MAKE) $@

.PHONY: install
