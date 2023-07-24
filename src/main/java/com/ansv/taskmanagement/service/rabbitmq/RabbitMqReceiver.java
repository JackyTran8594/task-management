package com.ansv.taskmanagement.service.rabbitmq;

import com.ansv.taskmanagement.dto.response.MemberDTO;
import com.ansv.taskmanagement.dto.response.UserDTO;
import com.ansv.taskmanagement.service.MemberService;
import com.ansv.taskmanagement.util.DataUtils;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.annotation.RabbitListenerConfigurer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistrar;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class RabbitMqReceiver implements RabbitListenerConfigurer {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMqReceiver.class);

    @Autowired
    private MemberService memberService;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public RabbitMqReceiver() {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
    }



    @RabbitListener(queues = {"${spring.rabbitmq.queue}"})
    public void receivedMessage(UserDTO user){
        logger.info("User Details Received is.. " + user.getUsername());
        MemberDTO member = memberService.findByUsername(user.getUsername());
        if(DataUtils.isNull(member)) {
            MemberDTO memberDTO = new MemberDTO();
            memberDTO.setEmail(user.getEmail());
            memberDTO.setName(user.getFullName());
            memberDTO.setEmail(user.getEmail());
            memberDTO.setUsername(user.getUsername());
            memberDTO.setPhone(user.getPhone_number());
            memberService.save(memberDTO);
        }
    }

    // receiving from human
    @RabbitListener(queues = {"${spring.rabbitmq.queue-task-human}"})
    public UserDTO receivedMessageFromHuman(UserDTO user){
        logger.info("User Details Received is from Human " + user.getUsername());
        return user;
    }
    // end

    // receiving from gateway
    @RabbitListener(queues = {"${spring.rabbitmq.queue-task}"})
    public void receivedMessageFromGateway(UserDTO user){
        logger.info("User Details Received is from Gateway " + user.getUsername());
        MemberDTO member = memberService.findByUsername(user.getUsername());
        if(DataUtils.isNull(member)) {
            MemberDTO memberDTO = new MemberDTO();
            memberDTO.setEmail(user.getEmail());
            memberDTO.setName(user.getFullName());
            memberDTO.setEmail(user.getEmail());
            memberDTO.setUsername(user.getUsername());
            memberDTO.setPhone(user.getPhone_number());
            memberService.save(memberDTO);
        }

    }
    // end



    @Override
    public void configureRabbitListeners(RabbitListenerEndpointRegistrar rabbitListenerEndpointRegistrar) {

    }

}
