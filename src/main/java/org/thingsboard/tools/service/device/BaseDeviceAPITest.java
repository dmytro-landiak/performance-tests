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
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.thingsboard.rest.client.RestClient;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.asset.Asset;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.relation.EntityRelation;
import org.thingsboard.server.common.data.relation.RelationTypeGroup;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public abstract class BaseDeviceAPITest implements DeviceAPITest {

    private static String dataAsStr = "{ \"alarmStateAVLconnectivity\": 0, \"alarmStateActionList\": 0, \"alarmStateApplicationVersion\": 0, \"alarmStateAvailableDiskSpace\": 0, \"alarmStateBattery\": 0, \"alarmStateCpuStatus\": 2, \"alarmStateDownloadedCDSetVersion\": 0, \"alarmStateDugiteCDSetVersion\": 0, \"alarmStateDugiteStatus\": 0, \"alarmStateEMVLevel3Version\": 0, \"alarmStateESN\": 0, \"alarmStateGateVehicleMode\": 0, \"alarmStateMemoryStatus\": 2, \"alarmStateNetworkStatus\": 2, \"alarmStateOSVersion\": 2, }";
    static byte[] data = dataAsStr.getBytes(StandardCharsets.UTF_8);

    static ObjectMapper mapper = new ObjectMapper();
    private static final Random random = new Random();

    static final int LOG_PAUSE = 1;
    static final int PUBLISHED_MESSAGES_LOG_PAUSE = 5;

    @Value("${entity.startIdx}")
    int entityStartIdx;

    @Value("${entity.endIdx}")
    int entityEndIdx;

    @Value("${rest.url}")
    String restUrl;

    @Value("${rest.username}")
    String username;

    @Value("${rest.password}")
    String password;

    RestClient restClient;

    int entityCount;

    final ExecutorService httpExecutor = Executors.newFixedThreadPool(100);
    final ScheduledExecutorService schedulerExecutor = Executors.newScheduledThreadPool(10);
    final ScheduledExecutorService schedulerLogExecutor = Executors.newScheduledThreadPool(10);

    private final List<EntityId> deviceIds = Collections.synchronizedList(new ArrayList<>());
    private final List<EntityId> assetIds = Collections.synchronizedList(new ArrayList<>());

    void init() {
        entityCount = entityEndIdx - entityStartIdx;
        restClient = new RestClient(restUrl);
        restClient.login(username, password);
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
        log.info("Creating {} devices...", entityCount);
        CountDownLatch latch = new CountDownLatch(entityCount);
        AtomicInteger count = new AtomicInteger();
        for (int i = entityStartIdx; i < entityEndIdx; i++) {
            final int tokenNumber = i;
            httpExecutor.submit(() -> {
                Device device = null;
                try {
                    String token = getToken(tokenNumber);
                    device = restClient.createDevice("Device " + token, "test-device");
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

        ScheduledFuture<?> logScheduleFuture = schedulerLogExecutor.scheduleAtFixedRate(() -> {
            try {
                log.info("{} devices have been created so far...", count.get());
            } catch (Exception ignored) {
            }
        }, 0, LOG_PAUSE, TimeUnit.SECONDS);

        ScheduledFuture<?> tokenRefreshScheduleFuture = schedulerLogExecutor.scheduleAtFixedRate(() -> {
            try {
                restClient.login(username, password);
            } catch (Exception ignored) {}
        }, 10, 10, TimeUnit.MINUTES);

        latch.await();
        tokenRefreshScheduleFuture.cancel(true);
        logScheduleFuture.cancel(true);
        log.info("{} devices have been created successfully!", deviceIds.size());

        restClient.getEntityGroupsByType(EntityType.DEVICE).stream()
                .filter(entityGroupInfo -> entityGroupInfo.getName().equals("Test Devices"))
                .findFirst().ifPresent(info -> restClient.addEntitiesToEntityGroup(info.getId(), deviceIds));
    }

    @Override
    public void createAssets() throws Exception {
        restClient.login(username, password);
        log.info("Creating {} assets...", entityCount);
        CountDownLatch latch = new CountDownLatch(entityCount);
        AtomicInteger count = new AtomicInteger();
        for (int i = entityStartIdx; i < entityEndIdx; i++) {
            final int tokenNumber = i;
            httpExecutor.submit(() -> {
                Asset asset = null;
                try {
                    String token = getToken(tokenNumber);
                    asset = restClient.createAsset("IP " + token, "IP");
                    assetIds.add(asset.getId());
                    count.getAndIncrement();
                } catch (Exception e) {
                    log.error("Error while creating asset", e);
                    if (asset != null && asset.getId() != null) {
                        restClient.getRestTemplate().delete(restUrl + "/api/asset/" + asset.getId().getId());
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        ScheduledFuture<?> logScheduleFuture = schedulerLogExecutor.scheduleAtFixedRate(() -> {
            try {
                log.info("{} assets have been created so far...", count.get());
            } catch (Exception ignored) {
            }
        }, 0, LOG_PAUSE, TimeUnit.SECONDS);

        ScheduledFuture<?> tokenRefreshScheduleFuture = schedulerLogExecutor.scheduleAtFixedRate(() -> {
            try {
                restClient.login(username, password);
            } catch (Exception ignored) {}
        }, 10, 10, TimeUnit.MINUTES);

        latch.await();
        tokenRefreshScheduleFuture.cancel(true);
        logScheduleFuture.cancel(true);
        log.info("{} assets have been created successfully!", deviceIds.size());

        restClient.getEntityGroupsByType(EntityType.ASSET).stream()
                .filter(entityGroupInfo -> entityGroupInfo.getName().equals("Installation Points"))
                .findFirst().ifPresent(info -> restClient.addEntitiesToEntityGroup(info.getId(), assetIds));
    }

    @Override
    public void removeDevices() throws Exception {
        restClient.login(username, password);
        log.info("Removing {} devices...", deviceIds.size());
        CountDownLatch latch = new CountDownLatch(deviceIds.size());
        AtomicInteger count = new AtomicInteger();
        for (EntityId deviceId : deviceIds) {
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

        ScheduledFuture<?> logScheduleFuture = schedulerLogExecutor.scheduleAtFixedRate(() -> {
            try {
                log.info("{} devices have been removed so far...", count.get());
            } catch (Exception ignored) {}
        }, 0, LOG_PAUSE, TimeUnit.SECONDS);

        ScheduledFuture<?> tokenRefreshScheduleFuture = schedulerLogExecutor.scheduleAtFixedRate(() -> {
            try {
                restClient.login(username, password);
            } catch (Exception ignored) {}
        }, 10, 10, TimeUnit.MINUTES);

        latch.await();
        logScheduleFuture.cancel(true);
        tokenRefreshScheduleFuture.cancel(true);
        Thread.sleep(1000);
        log.info("{} devices have been removed successfully! {} were failed for removal!", count.get(), deviceIds.size() - count.get());
    }

    @Override
    public void removeAssets() throws Exception {
        restClient.login(username, password);
        log.info("Removing {} assets...", assetIds.size());
        CountDownLatch latch = new CountDownLatch(assetIds.size());
        AtomicInteger count = new AtomicInteger();
        for (EntityId assetId : assetIds) {
            httpExecutor.submit(() -> {
                try {
                    restClient.getRestTemplate().delete(restUrl + "/api/asset/" + assetId.getId());
                    count.getAndIncrement();
                } catch (Exception e) {
                    log.error("Error while deleting asset", e);
                } finally {
                    latch.countDown();
                }
            });
        }

        ScheduledFuture<?> logScheduleFuture = schedulerLogExecutor.scheduleAtFixedRate(() -> {
            try {
                log.info("{} asset have been removed so far...", count.get());
            } catch (Exception ignored) {}
        }, 0, LOG_PAUSE, TimeUnit.SECONDS);

        ScheduledFuture<?> tokenRefreshScheduleFuture = schedulerLogExecutor.scheduleAtFixedRate(() -> {
            try {
                restClient.login(username, password);
            } catch (Exception ignored) {}
        }, 10, 10, TimeUnit.MINUTES);

        latch.await();
        logScheduleFuture.cancel(true);
        tokenRefreshScheduleFuture.cancel(true);
        Thread.sleep(1000);
        log.info("{} asset have been removed successfully! {} were failed for removal!", count.get(), assetIds.size() - count.get());
    }

    @Override
    public void createRelations() throws Exception {
        restClient.login(username, password);
        log.info("Creating relations...");

        Asset bus = restClient.findAsset("Bus 1").orElse(null);

        CountDownLatch latch = new CountDownLatch(assetIds.size());
        AtomicInteger count = new AtomicInteger();
        for (int i = entityStartIdx; i < entityEndIdx; i++) {
            EntityId assetId = assetIds.get(i);
            EntityId deviceId = deviceIds.get(i);
            httpExecutor.submit(() -> {
                try {
                    if (bus != null) {
                        EntityRelation relationToIp = new EntityRelation();
                        relationToIp.setTypeGroup(RelationTypeGroup.COMMON);
                        relationToIp.setFrom(bus.getId());
                        relationToIp.setTo(assetId);
                        relationToIp.setType("AssetToIP");
                        restClient.saveRelation(relationToIp);
                    }

                    EntityRelation relationToDevice = new EntityRelation();
                    relationToDevice.setTypeGroup(RelationTypeGroup.COMMON);
                    relationToDevice.setFrom(assetId);
                    relationToDevice.setTo(deviceId);
                    relationToDevice.setType("IPtoDevice");
                    restClient.saveRelation(relationToDevice);

                    count.getAndIncrement();
                } catch (Exception e) {
                    log.error("Error while creating relation", e);
                } finally {
                    latch.countDown();
                }
            });
        }

        ScheduledFuture<?> logScheduleFuture = schedulerLogExecutor.scheduleAtFixedRate(() -> {
            try {
                log.info("{} relations have been created so far...", count.get());
            } catch (Exception ignored) {}
        }, 0, LOG_PAUSE, TimeUnit.SECONDS);

        ScheduledFuture<?> tokenRefreshScheduleFuture = schedulerLogExecutor.scheduleAtFixedRate(() -> {
            try {
                restClient.login(username, password);
            } catch (Exception ignored) {}
        }, 10, 10, TimeUnit.MINUTES);

        latch.await();
        logScheduleFuture.cancel(true);
        tokenRefreshScheduleFuture.cancel(true);
        Thread.sleep(1000);
        log.info("{} relations have been created successfully! {} were failed for creation!", count.get(), assetIds.size() - count.get());
    }

    @Override
    public void saveAssetAttributes() throws Exception {
        restClient.login(username, password);
        log.info("Saving asset attributes...");

        CountDownLatch latch = new CountDownLatch(assetIds.size());
        AtomicInteger count = new AtomicInteger();
        for (EntityId entityId : assetIds) {
            httpExecutor.submit(() -> {
                try {
                    restClient.saveEntityAttributesV1(entityId, "SERVER_SCOPE", createAssetAttrRequest());
                    count.getAndIncrement();
                } catch (Exception e) {
                    log.error("Error while creating attributes", e);
                } finally {
                    latch.countDown();
                }
            });
        }

        ScheduledFuture<?> logScheduleFuture = schedulerLogExecutor.scheduleAtFixedRate(() -> {
            try {
                log.info("{} attributes have been created so far...", count.get());
            } catch (Exception ignored) {}
        }, 0, LOG_PAUSE, TimeUnit.SECONDS);

        ScheduledFuture<?> tokenRefreshScheduleFuture = schedulerLogExecutor.scheduleAtFixedRate(() -> {
            try {
                restClient.login(username, password);
            } catch (Exception ignored) {}
        }, 10, 10, TimeUnit.MINUTES);

        latch.await();
        logScheduleFuture.cancel(true);
        tokenRefreshScheduleFuture.cancel(true);
        Thread.sleep(1000);
        log.info("{} attributes have been created successfully! {} were failed for creation!", count.get(), assetIds.size() - count.get());
    }

    @Override
    public void saveDeviceAttributes() throws Exception {
        restClient.login(username, password);
        log.info("Saving device attributes...");

        CountDownLatch latch = new CountDownLatch(assetIds.size());
        AtomicInteger count = new AtomicInteger();
        for (EntityId entityId : deviceIds) {
            httpExecutor.submit(() -> {
                try {
                    restClient.saveEntityAttributesV1(entityId, "SERVER_SCOPE", createDeviceAttrRequest());
                    count.getAndIncrement();
                } catch (Exception e) {
                    log.error("Error while creating attributes", e);
                } finally {
                    latch.countDown();
                }
            });
        }

        ScheduledFuture<?> logScheduleFuture = schedulerLogExecutor.scheduleAtFixedRate(() -> {
            try {
                log.info("{} attributes have been created so far...", count.get());
            } catch (Exception ignored) {}
        }, 0, LOG_PAUSE, TimeUnit.SECONDS);

        ScheduledFuture<?> tokenRefreshScheduleFuture = schedulerLogExecutor.scheduleAtFixedRate(() -> {
            try {
                restClient.login(username, password);
            } catch (Exception ignored) {}
        }, 10, 10, TimeUnit.MINUTES);

        latch.await();
        logScheduleFuture.cancel(true);
        tokenRefreshScheduleFuture.cancel(true);
        Thread.sleep(1000);
        log.info("{} attributes have been created successfully! {} were failed for creation!", count.get(), assetIds.size() - count.get());
    }

    private ObjectNode createAssetAttrRequest() {
        ObjectNode node = mapper.createObjectNode();
        node.put("installationType", "mobile");
        node.put("polePosition", "polePosition");
        node.put("vehicleId", "vehicleId");
        node.put("vehicleSection", "sectionId");
        node.put("vehicleType", "bus");
        return node;
    }

    private ObjectNode createDeviceAttrRequest() {
        ObjectNode node = mapper.createObjectNode();
        node.put("dynamicConfiguration", "dynamicConfiguration");
        node.put("electronicSerialNumber", "electronicSerialNumber");
        node.put("staticConfiguration", "staticConfiguration");
        node.put("software", "software");
        node.put("firmware", "firmware");
        node.put("esn", "01000001");
        return node;
    }

    String generateNode() {
        ObjectNode node = mapper.createObjectNode();

        node.put("alarmStateAVLconnectivity", generateValue(0, 3));
        node.put("alarmStateActionList", generateValue(0, 3));
        node.put("alarmStateApplicationVersion", generateValue(0, 3));
        node.put("alarmStateAvailableDiskSpace", generateValue(0, 3));
        node.put("alarmStateBattery", generateValue(0, 3));
        node.put("alarmStateCpuStatus", generateValue(0, 3));
        node.put("alarmStateDownloadedCDSetVersion", generateValue(0, 3));
        node.put("alarmStateDugiteCDSetVersion", generateValue(0, 3));
        node.put("alarmStateDugiteStatus", generateValue(0, 3));
        node.put("alarmStateEMVLevel3Version", generateValue(0, 3));
        node.put("alarmStateESN", generateValue(0, 3));
        node.put("alarmStateGateVehicleMode", generateValue(0, 3));
        node.put("alarmStateMemoryStatus", generateValue(0, 3));
        node.put("alarmStateNetworkStatus", generateValue(0, 3));
        node.put("alarmStateOSVersion", generateValue(0, 3));

        try {
            return mapper.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            log.error("Failed to convert node!", e);
            return dataAsStr;
        }
    }

    int generateValue(int min, int max) {
        if (min >= max) {
            throw new IllegalArgumentException("Max value must be greater than min value!");
        }
        return random.nextInt((max - min) + 1) + min;
    }

}
