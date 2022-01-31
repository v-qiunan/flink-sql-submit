 CREATE TABLE datagen (
   id INT,
   name STRING
 ) WITH (
     'connector' = 'datagen'
 );

  CREATE TABLE print (
    id INT,
    name STRING
  ) WITH (
      'connector' = 'print'
  );

 insert into print select * from datagen;