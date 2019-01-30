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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.Netty4ClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.client.AsyncRestTemplate;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
@ConditionalOnProperty(prefix = "device", value = "api", havingValue = "HTTP")
public class DeviceHttpAPITest extends BaseDeviceAPITest {

    private final Random rand = new Random();

    private EventLoopGroup eventLoopGroup;
    private AsyncRestTemplate httpClient;

    @PostConstruct
    void init() {
        super.init();
        this.eventLoopGroup = new NioEventLoopGroup();
        Netty4ClientHttpRequestFactory nettyFactory = new Netty4ClientHttpRequestFactory(this.eventLoopGroup);
        httpClient = new AsyncRestTemplate(nettyFactory);
    }

    @PreDestroy
    void destroy() {
        super.destroy();
        if (this.eventLoopGroup != null) {
            this.eventLoopGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
        }
    }

    @Override
    public void runApiTests(int publishTelemetryCount, final int publishTelemetryPause) throws InterruptedException, JsonProcessingException {
        restClient.login(username, password);
        log.info("Starting performance test for {} devices...", deviceCount);
        long maxDelay = (publishTelemetryPause + 1) * publishTelemetryCount;
        final int totalMessagesToPublish = deviceCount * publishTelemetryCount;
        AtomicInteger totalPublishedCount = new AtomicInteger();
        AtomicInteger successPublishedCount = new AtomicInteger();
        AtomicInteger failedPublishedCount = new AtomicInteger();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        for (int i = deviceStartIdx; i < deviceEndIdx; i++) {
            String accessToken = deviceTokens.get(rand.nextInt(deviceTokens.size()));
            log.info("ACCESS_TOKEN - [{}]", accessToken);
            final int tokenNumber = i;
            testExecutor.submit(() -> {
                testPublishExecutor.scheduleAtFixedRate(() -> {
                    try {
                        String url = restUrl + "/api/v1/" + accessToken + "/telemetry";
                        HttpEntity<String> entity = new HttpEntity<>(mapper.writeValueAsString(generateMsg()), headers);
                        ListenableFuture<ResponseEntity<Void>> future = httpClient.exchange(url, HttpMethod.POST, entity, Void.class);
                        future.addCallback(new ListenableFutureCallback<ResponseEntity>() {
                            @Override
                            public void onFailure(Throwable throwable) {
                                failedPublishedCount.getAndIncrement();
                                log.error("Error while publishing telemetry, token: {}", tokenNumber, throwable);

                                totalPublishedCount.getAndIncrement();
                                logPublishedMessages(totalPublishedCount.get(), totalMessagesToPublish, tokenNumber);
                            }

                            @Override
                            public void onSuccess(ResponseEntity responseEntity) {
                                if (responseEntity.getStatusCode().is2xxSuccessful()) {
                                    successPublishedCount.getAndIncrement();
                                } else {
                                    failedPublishedCount.getAndIncrement();
                                    log.error("Error while publishing telemetry, token: {}, status code: {}", tokenNumber, responseEntity.getStatusCode().getReasonPhrase());
                                }

                                totalPublishedCount.getAndIncrement();
                                logPublishedMessages(totalPublishedCount.get(), totalMessagesToPublish, tokenNumber);
                            }
                        });
                    } catch (Exception e) {
                        log.error("Error while publishing telemetry, token: {}", tokenNumber, e);
                    }
                }, 0, publishTelemetryPause, TimeUnit.MILLISECONDS);
            });
        }
        Thread.sleep(maxDelay);
        testPublishExecutor.shutdownNow();
        log.info("Performance test was completed for {} devices!", deviceCount);
        log.info("{} messages were published successfully, {} failed!", successPublishedCount.get(), failedPublishedCount.get());
    }

    @Override
    public void runApiTestsOae(int publishTelemetryCount, final int publishTelemetryPause) throws InterruptedException, IOException {
        restClient.login(username, password);
        long maxDelay = (publishTelemetryPause + 1) * publishTelemetryCount;
        final int totalMessagesToPublish = publishTelemetryCount;
        AtomicInteger totalPublishedCount = new AtomicInteger();
        AtomicInteger successPublishedCount = new AtomicInteger();
        AtomicInteger failedPublishedCount = new AtomicInteger();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        final int tokenNumber = 0;
        testExecutor.submit(() -> {
            testPublishExecutor.scheduleAtFixedRate(() -> {
                try {
                    String accessToken = deviceTokens.get(rand.nextInt(deviceTokens.size()));
                    String url = restUrl + "/api/v1/" + accessToken + "/telemetry";
                    HttpEntity<String> entity = new HttpEntity<>(mapper.writeValueAsString(generateMsg()), headers);
                    ListenableFuture<ResponseEntity<Void>> future = httpClient.exchange(url, HttpMethod.POST, entity, Void.class);
                    future.addCallback(new ListenableFutureCallback<ResponseEntity>() {
                        @Override
                        public void onFailure(Throwable throwable) {
                            failedPublishedCount.getAndIncrement();
                            log.error("Error while publishing telemetry, token: {}", tokenNumber, throwable);

                            totalPublishedCount.getAndIncrement();
                            logPublishedMessages(totalPublishedCount.get(), totalMessagesToPublish, tokenNumber);
                        }

                        @Override
                        public void onSuccess(ResponseEntity responseEntity) {
                            if (responseEntity.getStatusCode().is2xxSuccessful()) {
                                successPublishedCount.getAndIncrement();
                            } else {
                                failedPublishedCount.getAndIncrement();
                                log.error("Error while publishing telemetry, token: {}, status code: {}", tokenNumber, responseEntity.getStatusCode().getReasonPhrase());
                            }

                            totalPublishedCount.getAndIncrement();
                            logPublishedMessages(totalPublishedCount.get(), totalMessagesToPublish, tokenNumber);
                        }
                    });
                } catch (Exception e) {
                    log.error("Error while publishing telemetry, token: {}", tokenNumber, e);
                }
            }, 0, publishTelemetryPause, TimeUnit.MILLISECONDS);
        });
        Thread.sleep(maxDelay);
        testPublishExecutor.shutdownNow();
        log.info("Performance test was completed for {} devices!", deviceCount);
        log.info("{} messages were published successfully, {} failed!", successPublishedCount.get(), failedPublishedCount.get());
    }

    private void logPublishedMessages(int count, int totalMessagesToPublish, int tokenNumber) {
        if (tokenNumber == deviceStartIdx) {
            log.info("[{}] messages have been published. [{}] messages to publish. Total [{}].",
                    count, totalMessagesToPublish - count, totalMessagesToPublish);
        }
    }

    @Override
    public void warmUpDevices() throws InterruptedException {
        restClient.login(username, password);
        log.info("Warming up devices...");
        CountDownLatch connectLatch = new CountDownLatch(deviceCount);
        int i = 0;
        for (String accessToken : deviceTokens) {
            final int tokenNumber = i;
            httpExecutor.submit(() -> {
                try {
                    restClient.getRestTemplate()
                            .postForEntity(restUrl + "/api/v1/{token}/telemetry",
                                    generateMsg(),
                                    ResponseEntity.class,
                                    accessToken);
                } catch (Exception e) {
                    log.error("Error while warming up device, token: {}", tokenNumber, e);
                } finally {
                    connectLatch.countDown();
                }
            });
            i++;
        }
        connectLatch.await();
    }

    private JsonNode generateMsg() throws JsonProcessingException {
        long randomMsgType = Math.round(Math.random());
        long randomAlarmStatus = Math.round(Math.random());

        String msgType;
        if (randomMsgType == 0) {
            msgType = "FIRE ALARM";
        } else {
            msgType = "MAINTENANCE";
        }

        long ts = generateRandomTs(1420113600000L, System.currentTimeMillis());
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        String time = simpleDateFormat.format(new Date(ts));

        return mapper.createObjectNode()
                .put("messageType", msgType)
//                .put("ateTagNo", "AUH-1001-234561")
                .put("errorCode", 110)
                .put("ateMode", 0)
                .put("alarmStatus", randomAlarmStatus)
                .put("timestamp", time);
    }

    private long generateRandomTs(long min, long max) {
        return (long) (Math.random() * ((max - min) + 1)) + min;
    }
}
