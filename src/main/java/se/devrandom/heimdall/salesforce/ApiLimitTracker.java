/*
 * Heimdall - Salesforce Backup Solution
 * Copyright (C) 2025 Johan Karlsteen
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package se.devrandom.heimdall.salesforce;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
@ConditionalOnExpression("!'${heimdall.api-limit.stop-at-percent:}'.isEmpty()")
public class ApiLimitTracker {
    private static final Logger log = LoggerFactory.getLogger(ApiLimitTracker.class);

    private static final Pattern USAGE_PATTERN = Pattern.compile("api-usage=(\\d+)/(\\d+)");
    private static final long LOG_INTERVAL_MS = 60_000;

    private final int stopAtPercent;

    private final AtomicLong used = new AtomicLong(0);
    private final AtomicLong dailyLimit = new AtomicLong(0);
    private final AtomicLong maxAllowed = new AtomicLong(0);
    private final AtomicLong usedAtStart = new AtomicLong(0);
    private final AtomicLong lastLogTime = new AtomicLong(0);

    private volatile boolean limitReached = false;
    private volatile boolean initialized = false;

    public ApiLimitTracker(@Value("${heimdall.api-limit.stop-at-percent}") int stopAtPercent) {
        this.stopAtPercent = stopAtPercent;
        log.info("ApiLimitTracker created: will stop at {}% of daily API limit", stopAtPercent);
    }

    public void initialize(long currentUsed, long limit) {
        this.usedAtStart.set(currentUsed);
        this.used.set(currentUsed);
        this.dailyLimit.set(limit);
        this.maxAllowed.set(limit * stopAtPercent / 100);
        this.initialized = true;

        log.info("ApiLimitTracker initialized: stop at {}% of daily API limit ({}/{}) | Currently used: {} ({}%)",
                stopAtPercent, maxAllowed.get(), limit, currentUsed,
                limit > 0 ? currentUsed * 100 / limit : 0);

        if (currentUsed >= maxAllowed.get()) {
            limitReached = true;
            log.warn("API LIMIT ALREADY REACHED at startup: used {} >= max allowed {} ({}% of {})",
                    currentUsed, maxAllowed.get(), stopAtPercent, limit);
        }
    }

    public void updateFromHeader(String sforceHeaderValue) {
        if (sforceHeaderValue == null) return;

        Matcher matcher = USAGE_PATTERN.matcher(sforceHeaderValue);
        if (!matcher.find()) return;

        try {
            long headerUsed = Long.parseLong(matcher.group(1));
            long headerLimit = Long.parseLong(matcher.group(2));

            used.set(headerUsed);
            if (headerLimit > 0) {
                dailyLimit.set(headerLimit);
                maxAllowed.set(headerLimit * stopAtPercent / 100);
            }

            if (!limitReached && headerUsed >= maxAllowed.get()) {
                limitReached = true;
                log.warn("API LIMIT REACHED: used {} >= max allowed {} ({}% of {})",
                        headerUsed, maxAllowed.get(), stopAtPercent, dailyLimit.get());
            }

            // Throttled logging - max once per 60 seconds
            long now = System.currentTimeMillis();
            long lastLog = lastLogTime.get();
            if (now - lastLog >= LOG_INTERVAL_MS && lastLogTime.compareAndSet(lastLog, now)) {
                logCurrentUsage();
            }

        } catch (NumberFormatException e) {
            log.debug("Failed to parse Sforce-Limit-Info header: {}", sforceHeaderValue);
        }
    }

    public boolean isLimitReached() {
        return limitReached;
    }

    public boolean isInitialized() {
        return initialized;
    }

    public void logCurrentUsage() {
        long currentUsed = used.get();
        long limit = dailyLimit.get();
        long max = maxAllowed.get();
        long pct = limit > 0 ? currentUsed * 100 / limit : 0;
        long usedSinceStart = currentUsed - usedAtStart.get();
        long remaining = Math.max(0, max - currentUsed);

        log.info("API Usage: {}/{} ({}%) | Budget: stop at {} ({}%) | Used this run: {} | Remaining budget: {}",
                currentUsed, limit, pct, max, stopAtPercent, usedSinceStart, remaining);
    }

    public long getUsedAtStart() {
        return usedAtStart.get();
    }

    public long getUsedNow() {
        return used.get();
    }

    public long getMaxAllowed() {
        return maxAllowed.get();
    }

    public long getDailyLimit() {
        return dailyLimit.get();
    }

    public int getStopAtPercent() {
        return stopAtPercent;
    }
}
