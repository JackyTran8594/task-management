package com.ansv.taskmanagement.controllers;


import com.ansv.taskmanagement.dto.response.TaskDTO;
import com.ansv.taskmanagement.dto.response.ResponseDataObject;
import com.ansv.taskmanagement.service.TaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.validation.Valid;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/api/task")
public class TaskController extends BaseController {

    @Autowired
    private TaskService TaskService;

    @GetMapping("")
    public ResponseEntity<ResponseDataObject<TaskDTO>> searchByCriteria(@RequestParam(name = "pageNumber") int pageNumber, @RequestParam(name = "pageSize") int pageSize, @RequestParam(name = "search") Optional<String> search) {
        ResponseDataObject<TaskDTO> response = new ResponseDataObject<>();
        Pageable page = pageRequest(new ArrayList<>(), pageNumber - 1, pageSize);
        Page<TaskDTO> listDTO = TaskService.findBySearchCriteria(search, page);
        // response
        response.pagingData = listDTO;
        response.success();
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @PostMapping("")
    public ResponseEntity<ResponseDataObject<TaskDTO>> create(@RequestBody @Valid TaskDTO item) {
        ResponseDataObject<TaskDTO> response = new ResponseDataObject<>();
        TaskDTO dto = TaskService.save(item);
        response.initData(dto);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @PutMapping("/{id}")
    public ResponseEntity<ResponseDataObject<TaskDTO>> update(@PathVariable(value = "id") Long id, @RequestBody @Valid TaskDTO item) {
        ResponseDataObject<TaskDTO> response = new ResponseDataObject<>();
        TaskDTO dto = TaskService.save(item);
        response.initData(dto);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/{id}")
    public ResponseEntity<ResponseDataObject<TaskDTO>> getById(@PathVariable(value = "id") Long id) {
        ResponseDataObject<TaskDTO> response = new ResponseDataObject<>();
        TaskDTO dto = TaskService.findById(id);
        response.initData(dto);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<ResponseDataObject<Integer>> deleteById(@PathVariable(value = "id") Long id) {
        ResponseDataObject<Integer> response = new ResponseDataObject<>();
        TaskService.deleteById(id);
        response.initData(1);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @PostMapping("/deleteByListId")
    public ResponseEntity<ResponseDataObject<Integer>> deleteByListId(@RequestBody List<Long> listId) {
        ResponseDataObject<Integer> response = new ResponseDataObject<>();
        Integer delete = TaskService.deleteByListId(listId);
        response.initData(delete);
        return new ResponseEntity<>(response, HttpStatus.OK);

    }

    @PostMapping("/uploadFileExcel")
    public ResponseEntity<ResponseDataObject<String>> uploadFileExcel(@RequestParam(name = "file") MultipartFile file) throws Exception {
        ResponseDataObject<String> response = new ResponseDataObject<>();
        
        response.initData("success");
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

}
