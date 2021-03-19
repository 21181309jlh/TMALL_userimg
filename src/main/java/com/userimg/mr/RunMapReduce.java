package com.userimg.mr;

/**
 * 执行MapReduce
 * @version 1.0.0
 * @Auther jialinhan
 * @CreatTime 2021/3/17 14:45
 */
public class RunMapReduce {
    public static void main(String[] args) throws Exception {
        //统计支付偏好
        SexDriver sexDriver=new SexDriver();
        sexDriver.run();
        AgeDriver ageDriver=new AgeDriver();
        ageDriver.run();
        ProvinceDriver provinceDriver=new ProvinceDriver();
        provinceDriver.run();
        PaylikeDriver paylikeDriver=new PaylikeDriver();
        paylikeDriver.run();
        ShoplikeDriver shoplikeDriver=new ShoplikeDriver();
        shoplikeDriver.run();
        ShopabilityDriver shopabilityDriver=new ShopabilityDriver();
        shopabilityDriver.run();

    }
}
