package com.userimg.service;


import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * 路径转发
 * @version 1.0.0
 * @Auther jialinhan
 * @CreatTime 2021/3/17 14:01
 */
@Controller
public class PathService {
    @RequestMapping("/")
    public String toIndex(){
        return "index";
    }
}
