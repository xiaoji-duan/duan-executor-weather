package com.xiaoji.duan.wti;

import java.util.Date;

import io.vertx.amqpbridge.AmqpBridge;
import io.vertx.amqpbridge.AmqpBridgeOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;

/**
 * 
 * 获取天气信息
 * 
 * @author xiaoji
 *
 */
public class MainVerticle extends AbstractVerticle {

	private WebClient client = null;
	private AmqpBridge bridge = null;
	private MongoClient mongodb = null;

	@Override
	public void start(Future<Void> startFuture) throws Exception {
		client = WebClient.create(vertx);

		JsonObject config = new JsonObject();
		config.put("host", "mongodb");
		config.put("port", 27017);
		config.put("keepAlive", true);
		mongodb = MongoClient.createShared(vertx, config);

		AmqpBridgeOptions options = new AmqpBridgeOptions();
		bridge = AmqpBridge.create(vertx, options);

		bridge.endHandler(endHandler -> {
			connectStompServer();
		});
		connectStompServer();
	}

	private void subscribeTrigger(String trigger) {
		MessageConsumer<JsonObject> consumer = bridge.createConsumer(trigger);
		System.out.println("Consumer " + trigger + " subscribed.");
		consumer.handler(vertxMsg -> this.process(trigger, vertxMsg));
	}

	private void process(String consumer, Message<JsonObject> received) {
		System.out.println("Consumer " + consumer + " received [" + received.body().encode() + "]");

		JsonObject data = received.body().getJsonObject("body");

		String locationid = data.getJsonObject("context").getString("locationid");
		String type = data.getJsonObject("context").getString("type");
		String date = data.getJsonObject("context").getString("date");
		String time = data.getJsonObject("context").getString("time");
		String next = data.getJsonObject("context").getString("next");

		Date today = new Date();
		
        java.text.DateFormat formatdate = new java.text.SimpleDateFormat("yyyyMMdd");
        java.text.DateFormat formattime = new java.text.SimpleDateFormat("hh:mm");
        
        date = formatdate.format(today);
        time = formattime.format(today);
        
		type = "default";
		weather(consumer, type, locationid, date, time, next, 1);

	}

	private void weather(String consumer, String type, String locationid, String date, String time, String nextTask, Integer retry) {
		String requesturi = config().getString("weather.uri.cityinfo", "http://www.weather.com.cn/data/cityinfo/##locationid##.html").replaceAll("##locationid##", locationid);
		
		mongodb.findOne("wti_location_weather", 
				new JsonObject()
				.put("locationid", locationid)
				.put("date", date)
				.put("time", time)
				.put("type", type),
				new JsonObject(),
				findOne -> {
			if (findOne.succeeded()) {
				
				JsonObject cached = findOne.result();
				
				if (cached != null && !cached.isEmpty()) {
					JsonObject nextctx = new JsonObject().put("context", new JsonObject().put("weather", cached));
	
					MessageProducer<JsonObject> producer = bridge.createProducer(nextTask);
					producer.send(new JsonObject().put("body", nextctx));
					System.out.println(
							"Consumer " + consumer + " send to [" + nextTask + "] result [" + nextctx.encode() + "]");
				} else {
					client.getAbs(requesturi).send(handler -> {
						if (handler.succeeded()) {
							HttpResponse<Buffer> resp = handler.result();
							
							String jsonstring = resp.bodyAsString();
							
							if (jsonstring != null && jsonstring.trim().startsWith("{") && jsonstring.trim().endsWith("}")) {
								JsonObject weather = new JsonObject(jsonstring);
								
								weather.put("locationid", locationid);
								weather.put("date", date);
								weather.put("time", weather.getJsonObject("weatherinfo", new JsonObject()).getString("time", time));
								weather.put("type", type);
								
								mongodb.save("wti_location_weather", weather, save -> {});
								
								JsonObject nextctx = new JsonObject().put("context", new JsonObject().put("weather", weather));
								
								MessageProducer<JsonObject> producer = bridge.createProducer(nextTask);
								producer.send(new JsonObject().put("body", nextctx));
								System.out.println(
										"Consumer " + consumer + " send to [" + nextTask + "] result [" + nextctx.encode() + "]");

							}
							
						} else {
							JsonObject nextctx = new JsonObject().put("context", new JsonObject().put("weather", new JsonObject()));
							
							MessageProducer<JsonObject> producer = bridge.createProducer(nextTask);
							producer.send(new JsonObject().put("body", nextctx));
							System.out.println(
									"Consumer " + consumer + " send to [" + nextTask + "] result [" + nextctx.encode() + "]");
						}
					});
				}
			} else {
				client.getAbs(requesturi).send(handler -> {
					if (handler.succeeded()) {
						HttpResponse<Buffer> resp = handler.result();
						
						String jsonstring = resp.bodyAsString();
						
						if (jsonstring != null && jsonstring.trim().startsWith("{") && jsonstring.trim().endsWith("}")) {
							JsonObject weather = new JsonObject(jsonstring);

							weather.put("locationid", locationid);
							weather.put("date", date);
							weather.put("time", weather.getJsonObject("weatherinfo", new JsonObject()).getString("time", time));
							weather.put("type", type);
							
							mongodb.save("wti_location_weather", weather, save -> {});
							
							JsonObject nextctx = new JsonObject().put("context", new JsonObject().put("weather", weather));
							
							MessageProducer<JsonObject> producer = bridge.createProducer(nextTask);
							producer.send(new JsonObject().put("body", nextctx));
							System.out.println(
									"Consumer " + consumer + " send to [" + nextTask + "] result [" + nextctx.encode() + "]");

						}
						
					} else {
						JsonObject nextctx = new JsonObject().put("context", new JsonObject().put("weather", new JsonObject()));
						
						MessageProducer<JsonObject> producer = bridge.createProducer(nextTask);
						producer.send(new JsonObject().put("body", nextctx));
						System.out.println(
								"Consumer " + consumer + " send to [" + nextTask + "] result [" + nextctx.encode() + "]");
					}
				});
			}
		});
	}
	
	private void connectStompServer() {
		bridge.start(config().getString("stomp.server.host", "sa-amq"), config().getInteger("stomp.server.port", 5672),
				res -> {
					if (res.failed()) {
						res.cause().printStackTrace();
						if (!config().getBoolean("debug", true)) {
							connectStompServer();
						}
					} else {
						System.out.println("Stomp server connected.");
						subscribeTrigger(config().getString("amq.app.id", "wti"));
					}
				});

	}
}
