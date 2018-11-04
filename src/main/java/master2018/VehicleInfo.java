package master2018;

public class VehicleInfo {
    private long timestamp;
    private  long VID;
    private long speed;
    private long xWay;
    private long lane;
    private long dir;
    private long segment;
    private long position;

    VehicleInfo(Long... values) {
        this.timestamp = values[0];
        this.VID = values[1];
        this.speed = values[2];
        this.xWay = values[3];
        this.lane = values[4];
        this.dir = values[5];
        this.segment = values[6];
        this.position = values[7];

    }
}
