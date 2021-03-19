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
 * @CreatTime 2021/3/18 13:11
 */
@Controller
public class AgeService {
    IHDFSDao ihdfsDao=new HDFSDaoImpl();
    @RequestMapping("/age")
    public String age(Model model) {
        try{
            //读取分析计算出的数据
            Map<String,Object>result=ihdfsDao.readFile("/data/userimgs/output/age/part-r-00000");
            //封装模型数据
            //System.out.println(result);
            model.addAttribute("from11to20",result.get("11-20"));
            model.addAttribute("from21to30",result.get("21-30"));
            model.addAttribute("from31to40",result.get("31-40"));
            model.addAttribute("from41to50",result.get("41-50"));
            model.addAttribute("from51to60",result.get("51-60"));
            model.addAttribute("from61to70",result.get("61-70"));
            model.addAttribute("from71to80",result.get("71-80"));
            //model.addAllAttributes(result);
            System.out.println("----------------------");
            //返回页面路径
            return "age";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }
}
