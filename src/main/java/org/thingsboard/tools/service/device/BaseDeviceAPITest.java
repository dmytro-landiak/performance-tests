package org.thingsboard.tools.service.device;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.thingsboard.client.tools.RestClient;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.id.DeviceId;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public abstract class BaseDeviceAPITest implements DeviceAPITest {

    static String dataAsStr = "{\"longKey\":73}";
    static byte[] data = dataAsStr.getBytes(StandardCharsets.UTF_8);

    static protected ObjectMapper mapper = new ObjectMapper();

    @Value("${device.startIdx}")
    int deviceStartIdx;

    @Value("${device.endIdx}")
    int deviceEndIdx;

    @Value("${rest.url}")
    String restUrl;

    @Value("${rest.username}")
    String username;

    @Value("${rest.password}")
    String password;

    RestClient restClient;

    int deviceCount;

    final ExecutorService httpExecutor = Executors.newFixedThreadPool(100);
    final ScheduledExecutorService testPublishExecutor = Executors.newScheduledThreadPool(10);
    final ExecutorService testExecutor = Executors.newFixedThreadPool(100);

    private final List<DeviceId> deviceIds = Collections.synchronizedList(new ArrayList<>());

    void init() {
        deviceCount = deviceEndIdx - deviceStartIdx;
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
        log.info("Creating {} devices...", deviceCount);
        CountDownLatch latch = new CountDownLatch(deviceCount);
        AtomicInteger count = new AtomicInteger();
        final int logInterval = deviceCount / 10;
        for (int i = deviceStartIdx; i < deviceEndIdx; i++) {
            final int tokenNumber = i;
            httpExecutor.submit(() -> {
                Device device = null;
                try {
                    device = restClient.createDevice("Device " + UUID.randomUUID(), "default");
                    String token = getToken(tokenNumber);
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
                if (count.get() > 0 && count.get() % logInterval == 0) {
                    log.info("{} devices has been created so far...", count.get());
                }
            });
        }
        latch.await();
        log.info("{} devices have been created successfully!", deviceIds.size());
    }

    @Override
    public void removeDevices() throws Exception {
        log.info("Removing {} devices...", deviceIds.size());
        CountDownLatch latch = new CountDownLatch(deviceIds.size());
        AtomicInteger count = new AtomicInteger();
        final int logInterval = deviceIds.size() / 10;
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
                if (count.get() > 0 && count.get() % logInterval == 0) {
                    log.info("{} devices has been removed so far...", count.get());
                }
            });
        }
        latch.await();
        Thread.sleep(1000);
        log.info("{} devices have been removed successfully! {} were failed for removal!", count.get(), deviceIds.size() - count.get());
    }

}
