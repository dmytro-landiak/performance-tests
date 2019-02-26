/**
 * Copyright © 2016-2018 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.tools.service.device;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.tools.service.email.EmailService;
import org.thingsboard.tools.service.websocket.WebSocketClientEndpoint;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public abstract class BaseDeviceAPITest implements DeviceAPITest {

    private static String dataAsStr = "{\"temperature\":30}";
    static byte[] data = dataAsStr.getBytes(StandardCharsets.UTF_8);

    static ObjectMapper mapper = new ObjectMapper();

    static final int LOG_PAUSE = 1;
    static final int PUBLISHED_MESSAGES_LOG_PAUSE = 5;

    private static final int MIN_NUMB = 0;
    private static final int MAX_NUMB = 50;

    @Value("${device.startIdx}")
    int deviceStartIdx;

    @Value("${device.endIdx}")
    int deviceEndIdx;

    @Value("${rest.url}")
    String restUrl;

    @Value("${rest.webSocketUrl}")
    String webSocketUrl;

    @Value("${rest.username}")
    String username;

    @Value("${rest.password}")
    String password;

    @Value("${performance.duration}")
    int duration;

    @Autowired
    private EmailService emailService;

    RestClient restClient;

    int deviceCount;

    final ExecutorService httpExecutor = Executors.newFixedThreadPool(100);
    final ScheduledExecutorService scheduledApiExecutor = Executors.newScheduledThreadPool(10);
    final ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(10);

    final Map<Integer, Long> subscriptionsMap = new ConcurrentHashMap<>();
    final Map<String, Integer> deviceSubIdsMap = new ConcurrentHashMap<>();

    private final List<DeviceId> deviceIds = Collections.synchronizedList(new ArrayList<>());

    private WebSocketClientEndpoint clientEndPoint;
    private boolean sendEmail;

    void init() {
        deviceCount = deviceEndIdx - deviceStartIdx;
        restClient = new RestClient(restUrl);
        restClient.login(username, password);
        try {
            clientEndPoint = new WebSocketClientEndpoint(new URI(webSocketUrl + restClient.getToken()));
            handleWebSocketMsg();
            scheduledExecutor.scheduleAtFixedRate(() -> {
                if (sendEmail) {
                    log.info("Sending an email in case of some troubles with the TB!");
//                    emailService.sendAlertEmail();
                    sendEmail = false;
                }
            }, 0, 15, TimeUnit.SECONDS);
        } catch (URISyntaxException e) {
            log.error("Bad URI provided...", e);
        }
        scheduledExecutor.scheduleAtFixedRate(() -> emailService.sendStatusEmail(), 1, 12, TimeUnit.HOURS);
    }

    private void handleWebSocketMsg() {
        clientEndPoint.addMessageHandler(message -> {
            log.info("Arrived message via WebSocket: {}", message);
            try {
                long currTs = System.currentTimeMillis();

                int subscriptionId = mapper.readTree(message).get("subscriptionId").asInt();
                if (subscriptionsMap.containsKey(subscriptionId)) {
                    long taskStartTs = subscriptionsMap.get(subscriptionId);
                    sendEmail = currTs - taskStartTs > duration;
                }
            } catch (IOException e) {
                log.warn("Failed to read message to json {}", message);
            }
        });
    }

    void destroy() {
        if (!this.httpExecutor.isShutdown()) {
            this.httpExecutor.shutdown();
        }
    }

    String getToken(int token) {
        return String.format("%20d", token).replace(" ", "0");
    }

    @Override
    public void createDevices() throws Exception {
        restClient.login(username, password);
        log.info("Creating {} devices...", deviceCount);
        CountDownLatch latch = new CountDownLatch(deviceCount);
        AtomicInteger count = new AtomicInteger();
        for (int i = deviceStartIdx; i < deviceEndIdx; i++) {
            final int tokenNumber = i;
            httpExecutor.submit(() -> {
                Device device = null;
                try {
                    String token = getToken(tokenNumber);
                    device = restClient.createDevice("Device " + token, "default");
                    restClient.updateDeviceCredentials(device.getId(), token);
                    deviceIds.add(device.getId());
                    count.getAndIncrement();
                } catch (Exception e) {
                    log.error("Error while creating device", e);
                    if (device != null && device.getId() != null) {
                        restClient.getRestTemplate().delete(restUrl + "/api/device/" + device.getId().getId());
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        ScheduledFuture<?> logScheduleFuture = scheduledExecutor.scheduleAtFixedRate(() -> {
            try {
                log.info("{} devices have been created so far...", count.get());
            } catch (Exception ignored) {
            }
        }, 0, LOG_PAUSE, TimeUnit.SECONDS);

        latch.await();
        logScheduleFuture.cancel(true);
        log.info("{} devices have been created successfully!", deviceIds.size());
    }

    @Override
    public void removeDevices() throws Exception {
        restClient.login(username, password);
        log.info("Removing {} devices...", deviceIds.size());
        CountDownLatch latch = new CountDownLatch(deviceIds.size());
        AtomicInteger count = new AtomicInteger();
        for (DeviceId deviceId : deviceIds) {
            httpExecutor.submit(() -> {
                try {
                    restClient.getRestTemplate().delete(restUrl + "/api/device/" + deviceId.getId());
                    count.getAndIncrement();
                } catch (Exception e) {
                    log.error("Error while deleting device", e);
                } finally {
                    latch.countDown();
                }
            });
        }

        ScheduledFuture<?> logScheduleFuture = scheduledExecutor.scheduleAtFixedRate(() -> {
            try {
                log.info("{} devices have been removed so far...", count.get());
            } catch (Exception ignored) {
            }
        }, 0, LOG_PAUSE, TimeUnit.SECONDS);

        latch.await();
        logScheduleFuture.cancel(true);
        Thread.sleep(1000);
        log.info("{} devices have been removed successfully! {} were failed for removal!", count.get(), deviceIds.size() - count.get());
    }

    @Override
    public void subscribeWebSockets() {
        int cmdId = 1;
        for (DeviceId deviceId : deviceIds) {
            subscribeToWebSocket(deviceId, cmdId);
            deviceSubIdsMap.putIfAbsent(restClient.getCredentials(deviceId).getCredentialsId(), cmdId);
            cmdId++;
        }
    }

    private void subscribeToWebSocket(DeviceId deviceId, int cmdId) {
        ObjectNode objectNode = mapper.createObjectNode();
        objectNode.put("entityType", "DEVICE");
        objectNode.put("entityId", deviceId.toString());
        objectNode.put("scope", "LATEST_TELEMETRY");
        objectNode.put("cmdId", Integer.toString(cmdId));

        ArrayNode arrayNode = mapper.createArrayNode();
        arrayNode.add(objectNode);

        ObjectNode resultNode = mapper.createObjectNode();
        resultNode.set("tsSubCmds", arrayNode);
        try {
            clientEndPoint.sendMessage(mapper.writeValueAsString(resultNode));
        } catch (Exception e) {
            log.error("Failed to send the message using WebSocket", e);
        }
    }

    byte[] generateByteData() {
        return generateStrData().getBytes(StandardCharsets.UTF_8);
    }

    String generateStrData() {
        ObjectNode node = mapper.createObjectNode().put("temperature", getRandomValue(MIN_NUMB, MAX_NUMB));
        try {
            return mapper.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            log.warn("Failed to write JSON as String [{}]", node);
        }
        return dataAsStr;
    }

    private int getRandomValue(int min, int max) {
        return (int) (Math.random() * ((max - min) + 1)) + min;
    }

}
