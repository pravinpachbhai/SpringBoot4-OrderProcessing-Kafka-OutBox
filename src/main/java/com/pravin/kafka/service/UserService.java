package com.pravin.kafka.service;

import com.pravin.kafka.dto.UserRequest;
import com.pravin.kafka.dto.UserResponse;
import com.pravin.kafka.entity.User;
import com.pravin.kafka.exception.ResourceNotFoundException;
import com.pravin.kafka.component.DataMapper;
import com.pravin.kafka.repository.UserRepository;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class UserService {

    private final UserRepository userRepository;
    private final DataMapper dataMapper;
    public UserService(UserRepository userRepository, DataMapper dataMapper) {
        this.userRepository = userRepository;
        this.dataMapper = dataMapper;
    }

    public UserResponse create(UserRequest request) {
        User user = dataMapper.toEntity(request);
        return dataMapper.toResponse(userRepository.save(user));
    }

    public UserResponse getById(Long id) {
        User user = userRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("User not found with id: " + id));
        return dataMapper.toResponse(user);
    }

    public List<UserResponse> getAll() {
        return userRepository.findAll()
                .stream()
                .map(dataMapper::toResponse)
                .toList();
    }
}