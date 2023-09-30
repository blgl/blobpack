OPTFLAGS = -Os
WARNFLAGS = -Wall -Wextra -Wno-parentheses
CFLAGS = $(OPTFLAGS) $(WARNFLAGS)
LDLIBS = -lsqlite3
EXEC = blobpack blobunpack

all:	$(EXEC)

blobpack.o:	blobpack.c packing.h

blobunpack.o:	blobunpack.c unpacking.h

packing.h:	packing.sql wrapsql
	perl wrapsql packing.sql >packing.h

unpacking.h:	unpacking.sql wrapsql
	perl wrapsql unpacking.sql >unpacking.h

clean:
	rm -rf $(EXEC) *.o *.dSYM *~

