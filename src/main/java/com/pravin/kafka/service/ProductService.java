package com.pravin.kafka.service;

import com.pravin.kafka.component.DataMapper;
import com.pravin.kafka.dto.ProductResponse;
import com.pravin.kafka.entity.Product;
import com.pravin.kafka.repository.ProductRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ProductService {
    private static final Logger log = LoggerFactory.getLogger(ProductService.class);
    private final ProductRepository productRepository;
    private final DataMapper dataMapper;
    public ProductService(ProductRepository productRepository, DataMapper dataMapper){
        this.productRepository = productRepository;
        this.dataMapper = dataMapper;
    }

    public List<ProductResponse> getAllProducts() {
        return productRepository.findAll().stream().map(dataMapper::toResponse).toList();
    }

}
