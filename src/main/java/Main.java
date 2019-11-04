import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.hash.Hashing;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

class Metric {
  public final String id;
  public final String ts;
  public final String key;
  public final JsonElement val;

  public Metric(String id, String ts, String key, JsonElement val) {
    this.id = id;
    this.ts = ts;
    this.key = key;
    this.val = val;
  }
}

public class Main {

  public static void main(String... args) throws Exception {
    new Main();
  }

  private final String tableName = "MyDynamo-Metrics6DBDA108-1SBGQBNBQOPFQ";
  private final AmazonDynamoDB dynamo = AmazonDynamoDBClientBuilder.defaultClient();
  private final int batch = 25;

  public Main() throws Exception {
    log("ctor");
    List<Metric> metrics = new ArrayList<>();
    for (int i = 0; i < 20000; ++i)
      metrics.add(randomMetric());
    writeMetrics(metrics);
  }

  public int writeMetrics(List<Metric> metrics) throws Exception {
    log("batchWrite", metrics.size());

    // R, C, V
    Table<Map.Entry<String/*id*/, String/*ts*/>, String/*key*/, JsonElement/*val*/> table = HashBasedTable.create();
    for (Metric metric : metrics)
      table.put(Maps.immutableEntry(metric.id, metric.ts), metric.key, metric.val);

    List<WriteRequest> writeRequests = new ArrayList<>();
    
    for (Map.Entry<Map.Entry<String, String>, Map<String, JsonElement>> idAndTs : table.rowMap().entrySet()) {
      String id = idAndTs.getKey().getKey();
      String ts = idAndTs.getKey().getValue();
      //
      ts = String.format("%s/%s", ts, Hashing.sha256().hashLong(new Random().nextLong()).toString().substring(0,7));
      //
      Map<String, AttributeValue> item = new HashMap<>();
      item.put("id", new AttributeValue(id));
      item.put("tskey", new AttributeValue(ts));
      for (Map.Entry<String, JsonElement> entry : idAndTs.getValue().entrySet())
        item.put(entry.getKey(), new AttributeValue(entry.getValue().getAsString())); //###TODO MAY WANT TO WRITE AS STRING OR NUMBER
      //
      writeRequests.add(new WriteRequest(new PutRequest(item)));
    }

    for (int fromIndex = 0; fromIndex < writeRequests.size(); fromIndex += batch) {
      // log(i);
      int toIndex = fromIndex + batch;
      if (writeRequests.size() < toIndex)
        toIndex = writeRequests.size();

      List<WriteRequest> subList = writeRequests.subList(fromIndex, toIndex);

      log(fromIndex, toIndex, subList.size());
      
      BatchWriteItemRequest batchWriteItemRequest = new BatchWriteItemRequest();
      batchWriteItemRequest.addRequestItemsEntry(tableName, subList);
      dynamo.batchWriteItem(batchWriteItemRequest);
    }

    return 200;
  }

  private Metric randomMetric() {
    final List<String> properties = ImmutableList.of("latitude", "longitude", "altitude", "velocity", "flOctets", "rlOctets");
    String id = String.format("arclight/%s.%s", new Random().nextInt(256), new Random().nextInt(256));
    // String ts = Instant.now().toString();
    String ts = Instant.ofEpochMilli(new Random().nextLong()).toString();
    String key = properties.get(new Random().nextInt(properties.size()));
    JsonElement val = new JsonPrimitive(new Random().nextLong());
    return new Metric(id, ts, key, val);
  }

  // private String random() {
  //   return UUID.randomUUID().toString();
  // }

  // private String random() {
  //   byte[] bytes = new byte[18];
  //   new SecureRandom().nextBytes(bytes);
  //   return BaseEncoding.base64Url().encode(bytes);
  // }

  private void log(Object... args) {
    new LogHelper(this).log(args);
  }

}