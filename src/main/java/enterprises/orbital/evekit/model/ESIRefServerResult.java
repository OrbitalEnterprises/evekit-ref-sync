package enterprises.orbital.evekit.model;

public class ESIRefServerResult<A> {
  /**
   * Time when this data expires.  This is normally determined from the ESI headers.
   */
  protected long expiryTime;

  /**
   * Opaque data object capturing the result of the server call.
   */
  protected A data;

  public ESIRefServerResult(long expiryTime, A data) {
    this.expiryTime = expiryTime;
    this.data = data;
  }

  public long getExpiryTime() {
    return expiryTime;
  }

  public A getData() {
    return data;
  }
}
