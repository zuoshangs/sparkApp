import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zuoshangs.spark.domain.model.Attr;
import com.zuoshangs.spark.domain.model.AttrVal;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by weike on 2018/4/11.
 */
public class Test1 {
    public static void main(String[] args) {
        Attr attr = new Attr();
        attr.setId(1);
        attr.setName("性别");
        List<AttrVal> attrValList = new ArrayList<>();
        AttrVal attrVal = new AttrVal();
        attrVal.setId(1);
        attrVal.setName("男");
        attrVal.setRule("{\"eq\":\"男\"}");
        attrValList.add(attrVal);
        AttrVal attrVal2 = new AttrVal();
        attrVal2.setId(1);
        attrVal2.setName("女");
        attrVal2.setRule("{\"eq\":\"女\"}");
        attrValList.add(attrVal2);
        attr.setAttrValList(attrValList);
        System.out.println(JSON.toJSONString(attr));
    }
}
