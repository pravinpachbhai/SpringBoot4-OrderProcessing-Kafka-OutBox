package com.pravin.kafka.component;

import com.pravin.kafka.dto.*;
import com.pravin.kafka.entity.*;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface DataMapper {

    // ===== USER =====
    User toEntity(UserRequest request);

    @Mapping(target = "addresses", source = "addresses")
    UserResponse toResponse(User user);

    // ===== ADDRESS =====
    Address toEntity(AddressRequest request);

    AddressResponse toResponse(Address address);

    // ===== ORDER =====
    @Mapping(target = "status", constant = "CREATED")
    Order toEntity(OrderRequest request);

    OrderResponse toResponse(Order order);

    // ===== ORDER ITEM =====
    OrderItem toEntity(OrderItemRequest request);

    OrderItemResponse toResponse(OrderItem item);

    // ===== INVENTORY =====
    InventoryResponse toResponse(Inventory inventory);

    // ===== PAYMENT =====
    Payment toEntity(PaymentRequest request);

    PaymentResponse toResponse(Payment payment);

    // ===== SHIPPING =====
    @Mapping(target = "status", constant = "CREATED")
    Shipment toEntity(ShipmentRequest request);

    ShipmentResponse toResponse(Shipment shipment);

    // ===== NOTIFICATION =====
    @Mapping(target = "status", constant = "SENT")
    Notification toEntity(NotificationRequest request);

    NotificationResponse toResponse(Notification notification);

    // ===== PRODUCT =====
    ProductResponse toResponse(Product product);

}
