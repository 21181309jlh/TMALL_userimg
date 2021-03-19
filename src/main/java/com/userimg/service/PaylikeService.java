package com.userimg.service;

import com.userimg.hdfs.IHDFSDao;
import com.userimg.hdfs.impl.HDFSDaoImpl;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import java.util.Map;
/**
 * 支付相关服务接口类
 * @version 1.0.0
 * @Auther jialinhan
 * @CreatTime 2021/3/17 15:15
 */
@Controller
public class PaylikeService {
    //创建HDFS访问接口的对象
    IHDFSDao ihdfsDao=new HDFSDaoImpl();

    /**
     * 支付偏好
     * @param model
     * @return
     */
    @RequestMapping("/paylike")
    public String paylike(Model model) {
        try{
            //读取分析计算出的数据
            Map<String,Object>result=ihdfsDao.readFile("/data/userimgs/output/paylike/part-r-00000");
            //封装模型数据

            //System.out.println(result);
            model.addAttribute("alipay",result.get("支付宝"));
            model.addAttribute("wechat",result.get("微信"));
            model.addAttribute("bank",result.get("银联"));
            model.addAttribute("others",result.get("其他"));
            //model.addAllAttributes(result);
            System.out.println("----------------------");
            //返回页面路径
            return "paylike";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }
}
