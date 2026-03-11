#!/bin/bash
echo "Esperando a que MongoDB inicie para configurar el Replica Set..."
sleep 5

mongosh -u admin -p testing --authenticationDatabase admin --eval "
try {
  rs.status();
  print('El Replica Set ya está inicializado.');
} catch (e) {
  print('Inicializando el Replica Set rs0...');
  rs.initiate({
    _id: 'rs0',
    members: [
      { _id: 0, host: 'mongodb_container:27017' }
    ]
  });
}
"