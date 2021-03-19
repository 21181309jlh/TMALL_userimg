package com.userimg.service;
import com.userimg.hdfs.IHDFSDao;
import com.userimg.hdfs.impl.HDFSDaoImpl;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import java.util.Map;
/**
 * @version 1.0.0
 * @Auther jialinhan
 * @CreatTime 2021/3/18 13:06
 */
@Controller
public class SexService {
    IHDFSDao ihdfsDao=new HDFSDaoImpl();
    @RequestMapping("/sex")
    public String sex(Model model) {
        try{
            //读取分析计算出的数据
            Map<String,Object>result=ihdfsDao.readFile("/data/userimgs/output/sex/part-r-00000");
            //封装模型数据
            //System.out.println(result);
            model.addAttribute("male",result.get("男"));
            model.addAttribute("female",result.get("女"));
            //model.addAllAttributes(result);
            System.out.println("----------------------");
            //返回页面路径
            return "sex";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }
}
