#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <pthread.h>



#define LEN_CODE_AIRPORT 3
#define STR_CODE_AIRPORT (LEN_CODE_AIRPORT+1) // Incluimos el caracter de final de palabra '\0'
#define NUM_AIRPORTS 303

#define COL_ORIGIN_AIRPORT 17
#define COL_DESTINATION_AIRPORT 18

#define MAXCHAR 500

/**
 * Reserva espacio para una matriz de tamaño nrow x ncol,
 * donde cada elemento de la matriz tiene size bytes
 */

void **malloc_matrix(int nrow, int ncol, size_t size)
{
  int i, j;

  void **ptr;

  ptr = malloc(sizeof(void *) * nrow);
  for(i = 0; i < nrow; i++) {
    ptr[i] = calloc(size * ncol, sizeof(int));
  }
    

  return ptr;
}

/**
 * Libera una matriz de tamaño con nrow filas. Utilizar
 * la funcion malloc_matrix para reservar la memoria 
 * asociada.
 */

void free_matrix(void **matrix, int nrow)
{
  int i;

  for(i = 0; i < nrow; i++)
    free(matrix[i]);
    
  free(matrix);
}

/**
 * Leer el fichero fname que contiene los codigos
 * IATA (3 caracteres) de los aeropuertos a analizar.
 * En total hay NUM_AIRPORTS a leer, un valor prefijado
 * (para simplificar el código). Los codigos de los
 * aeropuertos se alacenan en la matriz airports, una
 * matriz cuya memoria ha sido previamente reservada.
 */

void read_airports(char **airports, char *fname) 
{
  int i;
  char line[MAXCHAR];

  FILE *fp;

  /*
   * eow es el caracter de fin de palabra
   */
  char eow = '\0';

  fp = fopen(fname, "r");
  if (!fp) {
    printf("ERROR: could not open file '%s'\n", fname);
    exit(1);
  }

  i = 0;
  while (i < NUM_AIRPORTS)
  {
    fgets(line, 100, fp);
    line[3] = eow; 

    /* Copiamos los datos al vector */
    strcpy(airports[i], line);

    i++;
  }

  fclose(fp);
}

/**
 * Dada la matriz de con los codigos de los aeropuertos,
 * así como un código de aeropuerto, esta función retorna
 * la fila asociada al aeropuerto.
 */

int get_index_airport(char *code, char **airports)
{
  int i;

  for(i = 0; i < NUM_AIRPORTS; i++) 
    if (strcmp(code, airports[i]) == 0)
      return i;

  return -1;
}


/**
 * Dada la matriz num_flights, se imprimen por pantalla el
 * numero de destinos diferentes que tiene cada aeropuerto.
 */

void print_num_flights_summary(int **num_flights, char **airports)
{
  int i, j, num;

  for(i = 0; i < NUM_AIRPORTS; i++) 
  {
    num = 0;

    for(j = 0; j < NUM_AIRPORTS; j++)
    {
      if (num_flights[i][j] > 0)
        num++;
    }

    printf("Origin: %s -- Number of different destinations: %d\n", airports[i], num);
  }
}

/**
 * Esta funcion se utiliza para extraer informacion del fichero CSV que
 * contiene informacion sobre los vuelos. En particular, dada una linea
 * leida de fichero, la funcion extra el origen y destino de los vuelos.
 */

int extract_fields_airport(char *origin, char *destination, char *line) 
{
  /*Recorre la linea por caracteres*/
  char caracter;
  /* i sirve para recorrer la linea
   * iterator es para copiar el substring de la linea a char
   * coma_count es el contador de comas
   */
  int i, iterator, coma_count;
  /* start indica donde empieza el substring a copiar
   * end indica donde termina el substring a copiar
   * len indica la longitud del substring
   */
  int start, end, len;
  /* error nos permite saber si todos los campos son correctos
   * 1 hay error, 0 no hay error 
   */
  int error = 0;
  /* found se utiliza para saber si hemos encontrado los dos campos:
   * origen y destino
   */
  int found = 0;
  /*
   * eow es el caracter de fin de palabra
   */
  char eow = '\0';
  /*
   * contenedor del substring a copiar
   */
  char word[STR_CODE_AIRPORT];
  /*
   * Inicializamos los valores de las variables
   */
  start = 0;
  end = -1;
  i = 0;
  coma_count = 0;
  /*
   * Empezamos a contar comas
   */
  do {
    caracter = line[i++];
    if (caracter == ',') {
      coma_count ++;
      /*
       * Cogemos el valor de end
       */
      end = i;
      /*
       * Si es uno de los campos que queremos procedemos a copiar el substring
       */
      if (coma_count ==  COL_ORIGIN_AIRPORT || coma_count == COL_DESTINATION_AIRPORT) {
        /*
         * Calculamos la longitud, si es mayor que 1 es que tenemos 
         * algo que copiar
         */
        len = end - start;

        if (len > 1) {

          if (len > STR_CODE_AIRPORT) {
            printf("ERROR len code airport\n");
            exit(1);
          }

          /*
           * Copiamos el substring
           */
          for(iterator = start; iterator < end-1; iterator ++){
            word[iterator-start] = line[iterator];
          }
          /*
           * Introducimos el caracter de fin de palabra
           */
          word[iterator-start] = eow;
          /*
           * Comprobamos que el campo no sea NA (Not Available) 
           */
          if (strcmp("NA", word) == 0)
            error = 1;
          else {
            switch (coma_count) {
              case COL_ORIGIN_AIRPORT:
                strcpy(origin, word);
                found++;
                break;
              case COL_DESTINATION_AIRPORT:
                strcpy(destination, word);
                found++;
                break;
              default:
                printf("ERROR in coma_count\n");
                exit(1);
            }
          }

        } else {
          /*
           * Si el campo esta vacio erroramos la linea entera 
           */

          error = 1;
        }
      }
      start = end;
    }
  } while (caracter && error==0);

  if (found != 2)
    error = 1;

  return error;
}

/**
 * Dado un fichero CSV que contiene informacion entre multiples aeropuertos,
 * esta funcion lee cada linea del fichero y actualiza la matriz num_flights
 * para saber cuantos vuelos hay entre cada cuidad origen y destino.
 */ 



#define BUFFER_SIZE 10
#define CELL_LINES 100

// Cell struct
typedef struct cell {

    int nelems;
    char **lines;
    
} cell;

// Buffer
struct buffer {
  cell **cells;
  int read;
  int write;
  int counter;
};

// parametros para los consumidores
struct param {
  struct buffer *buffer;
  int **num_flights;
  char **airports;
};

pthread_mutex_t data = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t save = PTHREAD_MUTEX_INITIALIZER;

pthread_cond_t consumer = PTHREAD_COND_INITIALIZER;
pthread_cond_t producer = PTHREAD_COND_INITIALIZER;

int estado = 0;

void *th_consumer(void *arg) {
    
    char origin[STR_CODE_AIRPORT], destination[STR_CODE_AIRPORT];
    int error, index_orig, index_dest;
    char line[MAXCHAR]; 
    int i, j;
    
    // lectura de los parámetros
    struct param *info = (struct param *) arg;

    struct buffer *buffer = info->buffer;

    // crear un temp para evitar escribir en el mismo sitio
    cell *toRead, *bufferCell;

    // Reservamos espacio
    toRead = malloc(sizeof(cell));

    toRead->nelems = 0;

    toRead->lines = malloc(sizeof(char *) * CELL_LINES);

    for (j = 0; j < CELL_LINES; j++) {
      toRead->lines[j] = malloc(sizeof(char *) * MAXCHAR);  
    }

    while (!estado) {
      // Hacemos un lock al acceder a datos compartidos
      pthread_mutex_lock(&data);

      // Si no hay datos, esperamos a la señal del consumidor
      while ((buffer->counter == 0) && (!estado)) {
        pthread_cond_wait(&consumer, &data);
      }

      // Si ha acabado
      if ((buffer->counter == 0) && (estado)) {
        break;
      } else {
        // Leemos del buffer
        bufferCell = buffer->cells[buffer->read];
        buffer->cells[buffer->read] = toRead;
        toRead = bufferCell;

        // Aumentamos la posición del puntero de lectura
        buffer->read += 1;

        if (buffer->read == BUFFER_SIZE) {
          buffer->read = 0;
        }


        buffer->counter--;
        pthread_cond_signal(&producer);  
      }

      pthread_mutex_unlock(&data);
      
      // Procesamos los datos obtenidos
      index_orig = get_index_airport(origin, info->airports);
      index_dest = get_index_airport(destination, info->airports);

      for (i = 0; i < toRead->nelems; i++) {
        error = extract_fields_airport(origin, destination, toRead->lines[i]);
        if (!error) {

          index_orig = get_index_airport(origin, info->airports);
          index_dest = get_index_airport(destination, info->airports);

          if ((index_orig >= 0) && (index_dest >= 0)){
            // hacemos lock al editar datos compartidos
            pthread_mutex_lock(&save);
            info->num_flights[index_orig][index_dest]++;
            pthread_mutex_unlock(&save);
          }
        } 
      }       
    }

    // Liberamos espacio
    for (j = 0; j < CELL_LINES; j++) {
      free(toRead->lines[j]);  
    }

    free(toRead->lines);
    free(toRead);
}

void read_airports_data(int **num_flights, char **airports, char *fname) {
    char origin[STR_CODE_AIRPORT], destination[STR_CODE_AIRPORT];
    int error, index_orig, index_dest;
    char line[MAXCHAR]; 
    FILE *fp;
    
    // creamos el buffer
    struct buffer buffer;
    buffer.write = 0;
    buffer.read = 0;
    buffer.counter = 0;
    buffer.cells = malloc(BUFFER_SIZE * sizeof(cell));

    cell *toAdd;
    int i,j;
    // Reservamos espacio
    for (i = 0; i < BUFFER_SIZE; i++) {
      toAdd = malloc(sizeof(cell));
      buffer.cells[i] = toAdd;

      toAdd->nelems = 0;
      toAdd->lines = malloc(sizeof(char *) * CELL_LINES);
      
      for (j = 0; j < CELL_LINES; j++) {
        toAdd->lines[j] = malloc(sizeof(char *) * MAXCHAR);
      }
    }

    // Parámetros para los hilos consumidores
    struct param info = {&buffer, num_flights, airports};
    
    pthread_t th1, th2;
    
    // creación de los hilos consumidores
    pthread_create(&th1, NULL, th_consumer, (void*) &info);
    pthread_create(&th2, NULL, th_consumer, (void*) &info);

    // abrimos el fichero en modo lectura
    fp = fopen(fname, "r");
    if (!fp) {
      printf("ERROR: could not open '%s'\n", fname);
      exit(1);
    }
    

    fgets(line, MAXCHAR, fp);


    cell *producerCell = malloc(sizeof(cell));
    producerCell->nelems = 0;

    producerCell->lines = malloc(sizeof(char *) * CELL_LINES);
    for (j = 0; j < CELL_LINES; j++) {
      producerCell->lines[j] = malloc(sizeof(char *) * MAXCHAR);
    }

    cell *tmp;

    while (1) {
      // Leer el número de lineas
      while ((producerCell->nelems < CELL_LINES) && (fgets(producerCell->lines[producerCell->nelems], MAXCHAR, fp) != NULL)) {
        producerCell->nelems += 1;
      }
      if (producerCell->nelems > 0) {
        // Hacemos lock al acceder a datos compartidos
        pthread_mutex_lock(&data);
        // Si el buffer está lleno esperamos a la señal
        while (buffer.counter == BUFFER_SIZE) {
          pthread_cond_wait(&producer, &data);
        }

        
        tmp = buffer.cells[buffer.write];
        buffer.cells[buffer.write] = producerCell;
        producerCell = tmp;

        // incrementamos la posición del puntero de escritura
        buffer.write += 1;

        if (buffer.write == BUFFER_SIZE) {
          buffer.write = 0;
        }


        buffer.counter++;
        // avisar al consumidor que puede leer (dar la señal)
        pthread_cond_signal(&consumer);
        pthread_mutex_unlock(&data);
        producerCell->nelems = 0;

      } else {
        break;
      } 
    }

    // avisar a los consumidores que no habrá más datos
    pthread_mutex_lock(&data);
    estado = 1;
    pthread_cond_signal(&consumer);
    pthread_mutex_unlock(&data);

    // Liberamos el espacio reservado anteriormente
    for (j = 0; j < CELL_LINES; j++) {
      free(producerCell->lines[j]);
    }
    free(producerCell->lines);
    free(producerCell);


    pthread_join(th1, NULL);
    pthread_join(th2, NULL);

    // Liberamos la memoria reservada 
    cell *toRemove;

    for (i = 0; i < BUFFER_SIZE; i++) {
      toRemove = buffer.cells[i];
      for (j = 0; j < CELL_LINES; j++) {
        free(toRemove->lines[j]);
      }
      free(toRemove->lines);
      free(toRemove);
    }
    
    fclose(fp);
}

/**
 * Esta es la funcion principal que realiza los siguientes procedimientos
 * a) Lee los codigos IATA del fichero de aeropuertos
 * b) Lee la informacion de los vuelos entre diferentes aeropuertos y
 *    actualiza la matriz num_flights correspondiente.
 * c) Se imprime para cada aeropuerto origen cuantos destinos diferentes
 *    hay.
 * d) Se imprime por pantalla lo que ha tardado el programa para realizar
 *    todas estas tareas.
 */

int main(int argc, char **argv)
{
  char **airports;
  int **num_flights;

  if (argc != 3) {
    printf("%s <airport.csv> <flights.csv>\n", argv[0]);
    exit(1);
  }

  struct timeval tv1, tv2;

  // Tiempo cronologico 
  gettimeofday(&tv1, NULL);

  // Reserva espacio para las matrices
  airports    = (char **) malloc_matrix(NUM_AIRPORTS, STR_CODE_AIRPORT, sizeof(char));
  num_flights = (int **) malloc_matrix(NUM_AIRPORTS, NUM_AIRPORTS, sizeof(int));

  // Lee los codigos de los aeropuertos 
  read_airports(airports, argv[1]);

  // Lee los datos de los vuelos
  read_airports_data(num_flights, airports, argv[2]);
  
  // Imprime un resumen de la tabla
  print_num_flights_summary(num_flights, airports);

  // Libera espacio
  free_matrix((void **) airports, NUM_AIRPORTS);
  free_matrix((void **) num_flights, NUM_AIRPORTS);

  // Tiempo cronologico 
  gettimeofday(&tv2, NULL);

  // Tiempo para la creacion del arbol 
  printf("Tiempo para procesar el fichero: %f segundos\n",
      (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
      (double) (tv2.tv_sec - tv1.tv_sec));

  return 0;
}
