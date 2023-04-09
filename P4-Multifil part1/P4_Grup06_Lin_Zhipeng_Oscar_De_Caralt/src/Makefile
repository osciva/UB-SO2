default: test2

exercici: test2.o
	  gcc test2.o -o test2 -lpthread

test2.o: test2.c
	gcc -c test2.c

clean:
	$(RM) test2 *.o *~