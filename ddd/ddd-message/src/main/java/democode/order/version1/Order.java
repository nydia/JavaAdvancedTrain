package democode.order.version1;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import lombok.Getter;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * @author nydia
 * Created on 2021/10/18
 */
@Getter
public class Order {
    private long orderId;
    private int buyerId;
    private int sellerId;
    private BigDecimal shippingFee;
    private BigDecimal discountAmount;
    private Address address;
    private Set<OrderItem> orderItems;

    //空构造，只是为了方便演示
    public Order(){}

    public Order(long orderId,int buyerId ,int sellerId,Address address, Set<OrderItem> orderItems){
        this.orderId = orderId;
        this.buyerId = buyerId;
        this.sellerId = sellerId;
        this.address = address;
        this.orderItems = orderItems;
    }

    /**
     * 更新收货地址
     * @param address
     */
    public void updateAddress(Address address){
        this.address = address;
    }
    /**
     * 支付总额等于订单总额 + 运费 - 优惠金额
     * @return
     */
    public BigDecimal getPayAmount(){
        BigDecimal amount = getAmount();
        BigDecimal payAmount = amount.add(shippingFee);
        if(Objects.nonNull(this.discountAmount)){
            payAmount = payAmount.subtract(discountAmount);
        }
        return payAmount;
    }

    /**
     * 订单总价 = 订单商品的价格之和
     *    amount 可否设置为一个实体属性？
     */
    public BigDecimal getAmount(){
        return orderItems.stream()
                .map(OrderItem::getPrice)
                .reduce(BigDecimal.ZERO,BigDecimal::add);
    }


    /**
     * 运费不能为负
     * @param shippingFee
     */
    public void setShippingFee(BigDecimal shippingFee){
        Preconditions.checkArgument(shippingFee.compareTo(BigDecimal.ZERO) >= 0, "运费不能为负");
        this.shippingFee = shippingFee;
    }

    /**
     * 设置优惠
     * @param discountAmount
     */
    public void setDiscount(BigDecimal discountAmount){
        Preconditions.checkArgument(discountAmount.compareTo(BigDecimal.ZERO) >= 0, "折扣金额不能为负");
        this.discountAmount = discountAmount;
    }

    /**
     * 原则上，返回给外部的引用，都应该防止间接被修改
     * @return
     */
    public Set<OrderItem> getOrderItems() {
        return Collections.unmodifiableSet(orderItems);
    }
}
