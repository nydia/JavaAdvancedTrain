package com.nydia.ddd.order.domain.valueObject;

import com.nydia.ddd.base.model.BaseEntity;
import com.nydia.ddd.base.model.BaseValue;
import lombok.Data;

/**
 * @Auther: nydia_lhq@hotmail.com
 * @Date: 2024/8/3 01:28
 * @Description: 配送地址
 */
@Data
public class ShippingAddress extends BaseValue {

    private String address;

}
