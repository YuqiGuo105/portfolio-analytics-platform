package site.yuqi.analytics.aggregator.governance;

public class GovernanceException extends RuntimeException {
    private final int status;
    private final String code;

    public GovernanceException(int status, String code, String message) {
        super(message);
        this.status = status;
        this.code = code;
    }

    public int status() { return status; }
    public String code() { return code; }

    public static GovernanceException invalid() {
        return new GovernanceException(400, "invalid_request", "Invalid governance request");
    }
}
