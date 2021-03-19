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
 * @CreatTime 2021/3/18 13:17
 */
@Controller
public class ShoplikeService {
    IHDFSDao ihdfsDao=new HDFSDaoImpl();
    @RequestMapping("/shoplike")
    public String sex(Model model) {
        try{
            //读取分析计算出的数据
            Map<String,Object>result=ihdfsDao.readFile("/data/userimgs/output/shoplike/part-r-00000");
            //封装模型数据
            //System.out.println(result);
            model.addAttribute("electronic",result.get("数码电器"));
            model.addAttribute("life",result.get("日用百货"));
            model.addAttribute("clothes",result.get("服装"));
            model.addAttribute("foods",result.get("餐饮美食"));
            //model.addAllAttributes(result);
            System.out.println("----------------------");
            //返回页面路径
            return "shoplike";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }
}
