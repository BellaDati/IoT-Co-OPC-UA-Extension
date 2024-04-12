package com.belladati.iot.ext;

import com.belladati.iot.collector.common.Field;
import com.belladati.iot.collector.common.FieldType;
import com.belladati.iot.collector.receiver.endpoint.EndpointMessageProcessor;
import com.belladati.iot.collector.receiver.endpoint.ProcessedMessage;
import com.belladati.iot.collector.receiver.endpoint.ReceiverEndpoint;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.plc4x.java.PlcDriverManager;
import org.apache.plc4x.java.api.PlcConnection;
import org.apache.plc4x.java.api.messages.PlcReadRequest;
import org.apache.plc4x.java.api.messages.PlcReadResponse;
import org.apache.plc4x.java.api.types.PlcResponseCode;
import org.apache.plc4x.java.opcua.protocol.OpcuaField;
import org.apache.plc4x.java.opcua.readwrite.types.OpcuaDataType;
import org.apache.plc4x.java.opcua.readwrite.types.OpcuaIdentifierType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Sample implemenation of IOT API ReceiverEndpoint that consumes messages from OPC UA endpoint.
 *
 * Tested with MS OPC UA Docker image started by command
 * docker run --rm -it -p 50000:50000 -p 8080:8080 --name opcplc mcr.microsoft.com/iotedge/opc-plc:latest --pn=50000 --autoaccept --sph --sn=5 --sr=10 --st=uint --fn=5 --fr=1 --ft=uint --ctb --scn --lid --lsn --ref --gn=5 --ut  --ph localhost
 *
 * And JSON config
 * {
 * "interval": 1000,
 * "url": "opcua:tcp://localhost:50000/?discovery=false",
 * "mapping" : {
 * "one" : "ns=2;s=RandomSignedInt32;DINT",
 * "two" : "ns=2;s=SlowUInt2;DINT",
 * "three" : "ns=2;s=65e451f1-56f1-ce84-a44f-6addf176beaf;STRING"
 * }
 * }
 */
public class OpcUaEndpoint implements ReceiverEndpoint {

    private static final Logger log = LoggerFactory.getLogger(OpcUaEndpoint.class);

    private static PlcDriverManager driverManager = new PlcDriverManager();
    private PlcReadRequest.Builder builder;

    private Vertx vertx;
    private PlcConnection plcConnection;
    private Long timerId;
    private EndpointMessageProcessor endpointMessageProcessor;
    private JsonObject config;

    /**
     * Just store injected instances.
     * @param s
     * @param vertx
     * @param jsonObject - this is our config in form of JSON object with keys "url", "mapping" and "interval" where mapping are key-value pairs
     * @param endpointMessageProcessor
     */
    @Override
    public void init(String s, Vertx vertx, JsonObject jsonObject, EndpointMessageProcessor endpointMessageProcessor) {
        this.vertx = vertx;
        this.endpointMessageProcessor = endpointMessageProcessor;
        this.config = jsonObject;
    }

    /**
     * Try to connect to OPC UA endpoint and setup periodic polling
     * @param unused
     * @return
     */
    @Override
    public Future<Void> start(Void unused) {
        Promise<Void> startPromise = Promise.promise();

        vertx.executeBlocking(future -> {
            try {
                connectAndSetup();
                if (this.plcConnection.isConnected()) {
                    timerId = vertx.setPeriodic(Math.round(Double.parseDouble(config.getString("interval", "1000"))), v -> poll());
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                future.complete();
            }
        }, startPromise);

        return startPromise.future();
    }

    private void connectAndSetup() throws Exception {
        final String url = config.getString("url");
        if (url != null) {
            this.plcConnection = driverManager.getConnection(url); // "opcua:tcp://localhost:50000/?discovery=false"
            if (this.plcConnection.getMetadata().canRead()) {
                this.builder = plcConnection.readRequestBuilder();
                JsonObject mapping = new JsonObject(config.getString("mapping","{}"));
                mapping.forEach(e -> this.builder.addItem(e.getKey(), OpcuaField.of(e.getValue().toString())));
            } else {
                this.plcConnection.close();
            }
        }
    }

    private void poll() {
        if (!plcConnection.isConnected()) {
            try {
                connectAndSetup();
            } catch (Exception e) {
                stop(null);
            }
        }

        vertx.executeBlocking(f -> {
            final Set<PlcReadResponse> response = new HashSet<>();
            try {
                if (!this.plcConnection.isConnected()) {
                    connectAndSetup();
                }
                log.info("OPC UA - Checking for messages");

                CompletableFuture<? extends PlcReadResponse> responseFuture = this.builder.build().execute();
                responseFuture.whenComplete((plcReadResponse, throwable) -> {
                    if (throwable != null) {
                        log.error(throwable.getMessage(), throwable);
                    } else {
                        response.add(plcReadResponse);
                    }
                });
                responseFuture.get(Math.round(Double.parseDouble(config.getString("interval", "1000")))*10, TimeUnit.MILLISECONDS); // Wait 10times the interval
            } catch (Throwable e) {
                if (e instanceof TimeoutException) { // Make timeout reconnect upon next poll
                    log.error("OPC UA - {}", e.getMessage());
                    try {
                        plcConnection.close();
                    } catch (Exception x) {
                        log.error(x.getMessage(), x);
                    }
                } else {
                    log.error(e.getMessage(), e);
                }
            } finally {
                f.complete(response);
            }
        }, res -> {
            if (res.result() != null && !((Set<PlcReadResponse>)res.result()).isEmpty()) {
                ProcessedMessage pm = endpointMessageProcessor.processMessage(getBody(((Set<PlcReadResponse>) res.result()).iterator().next()));
                if (!pm.isError() && !pm.isIgnore()) {
                    endpointMessageProcessor.finishProcessing(pm);
                }
            }
        });
    }

    /**
     * Transform response into JSON object by reading all fields and creating flat JSON from them
     *
     * @param response
     * @return
     */
    private JsonObject getBody(PlcReadResponse response) {
        JsonObject o = new JsonObject();
        for (String fieldName : response.getFieldNames()) {
            if (response.getResponseCode(fieldName) == PlcResponseCode.OK) {
                int numValues = response.getNumberOfValues(fieldName);
                if (numValues == 1) {
                    o.put(fieldName, response.getObject(fieldName));
                }
                else {
                    JsonArray array = new JsonArray();
                    o.put(fieldName, array);
                    for(int i = 0; i < numValues; i++) {
                        array.add(response.getObject(fieldName, i));
                    }
                }
            } else {
                log.error(fieldName + ": " + response.getResponseCode(fieldName));
            }
        }
        log.info(o.encodePrettily());
        return o;
    }

    @Override
    public Future<Void> stop(Void unused) {
        if (isRunning()) {
            try {
                vertx.cancelTimer(timerId);
                timerId = null;
                plcConnection.close();
                plcConnection = null;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        return Future.succeededFuture();
    }

    @Override
    public boolean isRunning() {
        return timerId != null && plcConnection != null && plcConnection.isConnected();
    }

    /**
     * In this case killing this endpoint is the same as stopping.
     */
    @Override
    public void killEndpoint() {
        stop(null);
    }

    @Override
    public String receiverName() {
        return "OPC UA";
    }

    @Override
    public Map<String, Field> configurationFields() {
        Map fields = new HashMap();
        fields.put("interval", new Field("Interval", FieldType.NUMBER));
        fields.put("url", new Field("URL", FieldType.TEXT));
        fields.put("mapping", new Field("Mapping", FieldType.LONGTEXT));
        return fields;
    }
}
