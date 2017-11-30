package enterprises.orbital.evekit.model;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import enterprises.orbital.base.OrbitalProperties;

/**
 * Useful utility methods for dealing with model objects.
 */
public class ModelUtil {

  private static final ThreadLocal<DateFormat> dateFormat = OrbitalProperties.dateFormatFactory(new OrbitalProperties.DateFormatGenerator() {

    @Override
    public DateFormat generate() {
      SimpleDateFormat result = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
      result.setTimeZone(TimeZone.getTimeZone("UTC"));
      return result;
    }
  });

  public static boolean isExpired(
                                  long cachedUntil) {
    long now = OrbitalProperties.getCurrentTime();
    return cachedUntil < now;
  }

  public long expiresIn(
                        long cachedUntil) {
    return Math.max(cachedUntil - OrbitalProperties.getCurrentTime(), 0);
  }

  public static Date convertEpochToDate(
                                        long epochTime) {
    return new Date(epochTime);
  }

  public static String formatDate(
                                  Date asDate) {
    return dateFormat.get().format(asDate);
  }

  public static String formatDate(
                                  long epochTime) {
    return formatDate(convertEpochToDate(epochTime));
  }

  public static long dateNormalizer(
                                    long sourceDate) {
    return sourceDate <= 0 ? -1 : sourceDate;
  }

  public static long safeConvertDate(
                                     Date dt) {
    return dt == null ? -1 : dateNormalizer(dt.getTime());
  }
}
