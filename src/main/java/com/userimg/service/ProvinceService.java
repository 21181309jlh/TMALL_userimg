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
 * @CreatTime 2021/3/18 13:14
 */
@Controller
public class ProvinceService {
    IHDFSDao ihdfsDao=new HDFSDaoImpl();
    @RequestMapping("/province")
    public String province(Model model) {
        try{
            //读取分析计算出的数据
            Map<String,Object>result=ihdfsDao.readFile("/data/userimgs/output/province/part-r-00000");
            //封装模型数据
            //System.out.println(result);
            model.addAttribute("shanghai",result.get("上海市"));
            model.addAttribute("yunnan",result.get("云南省"));
            model.addAttribute("neimenggu",result.get("内蒙古自治区"));
            model.addAttribute("beijing",result.get("北京市"));
            model.addAttribute("taiwan",result.get("台湾省"));
            model.addAttribute("jilin",result.get("吉林省"));
            model.addAttribute("sichuan",result.get("四川省"));
            model.addAttribute("tianjin",result.get("天津市"));
            model.addAttribute("ningxia",result.get("宁夏回族自治区"));
            model.addAttribute("anhui",result.get("安徽省"));
            model.addAttribute("shandong",result.get("山东省"));
            model.addAttribute("guangdong",result.get("广东省"));
            model.addAttribute("guangxi",result.get("广西壮族自治区"));
            model.addAttribute("xinjiang",result.get("新疆维吾尔自治区"));
            model.addAttribute("jiangsu",result.get("江苏省"));
            model.addAttribute("jiangxi",result.get("江西省"));
            model.addAttribute("hebei",result.get("河北省"));
            model.addAttribute("henan",result.get("河南省"));
            model.addAttribute("zhejiang",result.get("浙江省"));
            model.addAttribute("hainan",result.get("海南省"));
            model.addAttribute("hubei",result.get("湖北省"));
            model.addAttribute("hunan",result.get("湖南省"));
            model.addAttribute("gansu",result.get("甘肃省"));
            model.addAttribute("fujian",result.get("福建省"));
            model.addAttribute("xizang",result.get("西藏自治区"));
            model.addAttribute("guizhou",result.get("贵州省"));
            model.addAttribute("liaoning",result.get("辽宁省"));
            model.addAttribute("chongqing",result.get("重庆市"));
            model.addAttribute("shanxi",result.get("陕西省"));
            model.addAttribute("qinghai",result.get("青海省"));
            model.addAttribute("heilongjiang",result.get("黑龙江省"));
            model.addAttribute("shanxi1",result.get("山西省"));
            //model.addAllAttributes(result);
            System.out.println("----------------------");
            //返回页面路径
            return "province";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }
}
