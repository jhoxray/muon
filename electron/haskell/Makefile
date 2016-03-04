# I am a comment, and I want to say that the variable CC will be
# the compiler to use.
CC=stack exec -- hastec
# Hey!, I am comment number 2. I want to say that CFLAGS will be the
# options I'll pass to the compiler.
HASTEFLAGS =--outdir=dll/ 

all: client

client: FunCharts.hs Highcharts.hs client.hs
	@echo "Compiling files for the client"
	$(CC) $(HASTEFLAGS) client.hs -o ../app/client.js

clean:
	@echo "Cleaning up..."
	rm dll/* *.o *.hi