# cstore_fdw/Makefile
#
# Copyright (c) 2016 Citus Data, Inc.
#

MODULE_big = cstore_fdw

PG_CPPFLAGS = --std=c99
SHLIB_LINK = -lprotobuf-c
OBJS = cstore.pb-c.o cstore_fdw.o cstore_writer.o cstore_reader.o \
       cstore_metadata_serialization.o cstore_compression.o murmur3.o cstore_bloomfilter.o

EXTENSION = cstore_fdw
DATA = cstore_fdw--1.7.sql cstore_fdw--1.6--1.7.sql  cstore_fdw--1.5--1.6.sql cstore_fdw--1.4--1.5.sql \
	   cstore_fdw--1.3--1.4.sql cstore_fdw--1.2--1.3.sql cstore_fdw--1.1--1.2.sql \
	   cstore_fdw--1.0--1.1.sql

REGRESS = create load query analyze data_types functions block_filtering drop \
		  insert copyto alter truncate
EXTRA_CLEAN = cstore.pb-c.h cstore.pb-c.c data/*.cstore data/*.cstore.footer \
              sql/block_filtering.sql sql/create.sql sql/data_types.sql sql/load.sql \
              sql/copyto.sql expected/block_filtering.out expected/create.out \
              expected/data_types.out expected/load.out expected/copyto.out

ifeq ($(enable_coverage),yes)
	PG_CPPFLAGS += --coverage
	SHLIB_LINK  += --coverage
	EXTRA_CLEAN += *.gcno
endif

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
	PG_CPPFLAGS += -I/usr/local/include
endif

#
# LZ4 compression can be enabled by passing enable_lz4=yes on the commandline
# additionaly the path to the lz4 folder can be supplied using the lz4_path variable
#

ifneq (,$(wildcard ../lz4/lib/liblz4.a))
	enable_lz4=yes
	lz4_path=../lz4
endif
ifeq ($(enable_lz4),yes)
	PG_CPPFLAGS += -DENABLE_LZ4
        SHLIB_LINK += -llz4
endif
ifneq ($(lz4_path),)
	PG_CPPFLAGS += -I$(lz4_path)/lib
        SHLIB_LINK += -L$(lz4_path)/lib
endif

#
# ZSTD compression can be enabled by passing enable_zstd=yes on the commandline
# additionaly the path to the zstd folder can be supplied using the zstd_path variable
#

ifneq (,$(wildcard ../zstd/lib/libzstd.a))
	enable_zstd=yes
	zstd_path=../zstd
endif
ifeq ($(enable_zstd),yes)
	PG_CPPFLAGS += -DENABLE_ZSTD
        SHLIB_LINK += -lzstd
endif
ifneq ($(zstd_path),)
	PG_CPPFLAGS += -I$(zstd_path)/lib
        SHLIB_LINK += -L$(zstd_path)/lib
endif

#
# Users need to specify their Postgres installation path through pg_config. For
# example: /usr/local/pgsql/bin/pg_config or /usr/lib/postgresql/9.3/bin/pg_config
#

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

ifndef MAJORVERSION
    MAJORVERSION := $(basename $(VERSION))
endif

ifeq (,$(findstring $(MAJORVERSION), 9.3 9.4 9.5 9.6 10 11 12 13))
    $(error PostgreSQL 9.3 to 13 is required to compile this extension)
endif

cstore.pb-c.c: cstore.proto
	protoc-c --c_out=. cstore.proto

installcheck: remove_cstore_files

remove_cstore_files:
	rm -f data/*.cstore data/*.cstore.footer
