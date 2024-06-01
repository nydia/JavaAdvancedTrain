package com.izhengyin.dddmessage.application;

import com.izhengyin.dddmessage.domain.aggregate.notice.AppMessagePublisher;
import com.izhengyin.dddmessage.domain.aggregate.notice.SocketMessagePublisher;
import com.izhengyin.dddmessage.domain.aggregate.notice.entity.valueobject.AppMessage;
import com.izhengyin.dddmessage.domain.aggregate.notice.entity.valueobject.SocketMessage;
import com.izhengyin.dddmessage.domain.shared.enums.Application;
import com.izhengyin.dddmessage.domain.shared.facade.ApnsServiceFacade;
import com.izhengyin.dddmessage.domain.shared.facade.HuaweiServiceFacade;
import com.izhengyin.dddmessage.domain.shared.facade.ImSocketServiceFacade;
import org.springframework.stereotype.Component;

/**
 * @author zhengyin
 * Created on 2021/7/28
 */
@Component
public class MessagePublisher implements AppMessagePublisher, SocketMessagePublisher {
    private final ApnsServiceFacade apnsServiceFacade;
    private final HuaweiServiceFacade huaweiServiceFacade;
    private final ImSocketServiceFacade imSocketServiceFacade;

    public MessagePublisher(ApnsServiceFacade apnsServiceFacade, HuaweiServiceFacade huaweiServiceFacade, ImSocketServiceFacade imSocketServiceFacade) {
        this.apnsServiceFacade = apnsServiceFacade;
        this.huaweiServiceFacade = huaweiServiceFacade;
        this.imSocketServiceFacade = imSocketServiceFacade;
    }

    @Override
    public void publish(AppMessage appMessage) {
        //根据Application 选择推送的渠道，当application多了以后可以通过策略模式来优化
        if (Application.IOS_XXX.equals(appMessage.getApplication())) {
            apnsServiceFacade.publish(appMessage);
        } else if (Application.ANDROID_XXX.equals(appMessage.getApplication())) {
            huaweiServiceFacade.publish(appMessage);
        }
    }

    @Override
    public void publish(SocketMessage socketMessage) {
        imSocketServiceFacade.publish(socketMessage);
    }
}