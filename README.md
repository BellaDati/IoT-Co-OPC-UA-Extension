# Belladati OPC UA Protocol Extension

Uses PLC4X framework to connect to OPC UA endpoint.

Tested with MS Docker image using command
```
docker run --rm -it -p 50000:50000 -p 8080:8080 --name opcplc mcr.microsoft.com/iotedge/opc-plc:latest --pn=50000 --autoaccept --sph --sn=5 --sr=10 --st=uint --fn=5 --fr=1 --ft=uint --ctb --scn --lid --lsn --ref --gn=5 --ut  --ph localhost
```

and endpoint config
* URL: opcua:tcp://localhost:50000/?discovery=false
* Interval: 1000  
* Mapping:
```json
{
"one": "ns=2;s=RandomSignedInt32;DINT",
"two": "ns=2;s=SlowUInt2;DINT",
"three": "ns=2;s=65e451f1-56f1-ce84-a44f-6addf176beaf;STRING"
}
```