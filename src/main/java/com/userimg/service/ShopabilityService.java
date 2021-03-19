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
 * @CreatTime 2021/3/18 13:27
 */
@Controller
public class ShopabilityService {
    IHDFSDao ihdfsDao=new HDFSDaoImpl();
    @RequestMapping("/shopability")
    public String sex(Model model) {
        try{
            //读取分析计算出的数据
            Map<String,Object>result=ihdfsDao.readFile("/data/userimgs/output/shopability/part-r-00000");
            //封装模型数据
            //System.out.println(result);
            model.addAttribute("from0to100",result.get("0-100"));
            model.addAttribute("from101to200",result.get("101-200"));
            model.addAttribute("from201to300",result.get("201-300"));
            model.addAttribute("from301to400",result.get("301-400"));
            model.addAttribute("from401to500",result.get("401-500"));
            model.addAttribute("from501to600",result.get("501-600"));
            model.addAttribute("from601to700",result.get("601-700"));
            model.addAttribute("from701to800",result.get("701-800"));
            model.addAttribute("from801to900",result.get("801-900"));
            model.addAttribute("from901to1000",result.get("901-1000"));
            model.addAttribute("from1001to3000",result.get("1001-3000"));
            model.addAttribute("from3001to5000",result.get("3001-5000"));
            model.addAttribute("largethan5000",result.get(">5000"));
            //model.addAllAttributes(result);
            System.out.println("----------------------");
            //返回页面路径
            return "shopability";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }
}
