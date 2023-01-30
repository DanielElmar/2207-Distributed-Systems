import java.util.logging.Logger;

public class RebalanceTimer extends Thread{

    private final Controller controller;
    private final int rebalancePeriod;
    private final Object obj;

    private Logger logger = Logger.getLogger(String.valueOf(RebalanceTimer.class));

    public RebalanceTimer( Controller controller, int rebalancePeriod){
        this.controller = controller;
        this.rebalancePeriod = rebalancePeriod;
        this.obj = new Object();
    }

    @Override
    public void run() {
        clock();
    }

    private synchronized void clock(){
        while (true) {
            try {
                //TimeUnit.SECONDS.sleep(rebalancePeriod);
                wait(rebalancePeriod * 1000);
                System.out.println("[REBALANCE] Timer Triggered");
                controller.rebalanceActive = true;
                //wait for store to complete

                var activeStoreOrRemove = true;
                while (activeStoreOrRemove){
                    activeStoreOrRemove = false;
                    for ( ControllerConnectionHandler handler: controller.connectionHandlers) {
                        if(handler.storeOrRemoveActive){
                            activeStoreOrRemove = true;
                        }
                    }
                    System.out.println("[REBALANCE] Waiting for store or Remove to finish");
                    try {
                        wait(5);
                    }catch (InterruptedException ignored){ }
                }

                controller.triggerRebalance();
            } catch (InterruptedException ignored) {
                System.out.println("[REBALANCE] Timer Reset");
            }
        }
    }
}
