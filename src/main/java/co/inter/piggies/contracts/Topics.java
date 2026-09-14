package co.inter.piggies.contracts;

public final class Topics {
    public static final String WITHDRAW_CMD_PREFIX = "piggies.withdraw.cmd.";
    public static final String DEPOSIT_CMD_PREFIX  = "piggies.deposit.cmd.";
    public static final String WITHDRAW_EVT = "piggies.withdraw.evt";
    public static final String DEPOSIT_EVT  = "piggies.deposit.evt";

    private Topics() {}
    public static String withdrawCmd(String country) { return WITHDRAW_CMD_PREFIX + country; }
    public static String depositCmd(String country)  { return DEPOSIT_CMD_PREFIX + country; }
}
