package democode.aggregation.order;

import lombok.Getter;

/**
 * @author nydia
 * Created on 2021/10/18
 */
@Getter
public class Agreement {
    private int agreementId;
    private int buyerId;
    private int sellerId;
    private Status status;
    //退货/退款协议状态
    public interface Status {

        String REFUNDING = "Refunding"; //申请退款中
        String REFUND_REJECTED = "RefundRejected";  //拒绝退款中
    }
    public void refunding(){
        //更新退货退款协议状态
    }
}
