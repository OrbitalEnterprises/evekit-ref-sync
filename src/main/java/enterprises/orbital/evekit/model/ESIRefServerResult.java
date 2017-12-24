package enterprises.orbital.evekit.model;

public class ESIRefServerResult {
  /**
   * Time when this data expires.  This is normally determined from the ESI headers.
   */
  protected long expiryTime;

  /**
   * Opaque data object capturing the result of the server call.
   */
  protected Object data;

  public ESIRefServerResult(long expiryTime, Object data) {
    this.expiryTime = expiryTime;
    this.data = data;
  }

  public long getExpiryTime() {
    return expiryTime;
  }

  public Object getData() {
    return data;
  }
}
