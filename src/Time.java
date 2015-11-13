import java.time.ZoneId;
import java.util.Calendar;
import java.util.TimeZone;

class Time {
    public static final TimeZone UTC_ZONE = TimeZone.getTimeZone("UTC");
    public static final ThreadLocal<Calendar> UTC_CALENDAR = ThreadLocal.withInitial(() -> Calendar.getInstance(UTC_ZONE));
    public static final ZoneId UTC_ZONE_ID = ZoneId.of("UTC");
}
