package com.powere2e.sco.scheduleTask.task.merchandise;

import cn.hutool.core.io.resource.ResourceUtil;
import com.alibaba.fastjson.JSONArray;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.powere2e.sco.scheduleTask.common.constant.BaseConstant;
import com.powere2e.sco.scheduleTask.common.utils.FieldCopyUtil;
import com.powere2e.sco.scheduleTask.common.utils.JsonMapperUtil;
import com.powere2e.sco.scheduleTask.domain.MerchandiseDomain;
import com.powere2e.sco.scheduleTask.dto.MerAndSupplierDTO;
import com.powere2e.sco.scheduleTask.service.common.MerchandiseService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * @ClassName MerchandiseDataCopyByJsonFile
 * @Description
 *   读取json文件实现商品数据的拷贝覆盖
 * @Author peng tian yang
 * @Date 2023/4/7 14:28
 * @Version 1.0
 */
@Slf4j
@Component
public class MerchandiseDataCopyByJsonFile {

    final String concatSign = BaseConstant.CONCAT_SIGN;

    @Autowired
    MerchandiseService merchandiseService;

    /**
     *
     * @param merBackupJson  商品备份表 json名称
     * @param merJson  商品表 json名称
     */
    public void dataCopyFromBackup(String merBackupJson, String merJson){

        //  jsonData/MERCHANDISE_BAK_prod_20230406.json
        //   测试
        Set<MerchandiseDomain> dataSource = getDataSet(
                merBackupJson);

        // guava Multimap
        Multimap<String, MerchandiseDomain> multiSource = ArrayListMultimap.create();

        // guava Multimap
        //   jsonData/MERCHANDISE_prod_20230407.json
        //   testJsonData/MERCHANDISE_test.json        测试
        Multimap<String, MerchandiseDomain> multiTarget = getDataMultimap(
                merJson);

        //
        dataSource.stream().forEach(
                merVal ->{
                    // 拼接一个唯一的 key
                    String key = merVal.getMerchandiseCode().concat(concatSign)
                            .concat(merVal.getSupplierCode());
                    multiSource.put(key,merVal);
                });

        // 要被替换的旧数据 (目标表已经存在的)
        List<MerchandiseDomain> replaceOld = new ArrayList<>();

        // 新数据(来自备份表)
        List<MerchandiseDomain> replaceNew = new ArrayList<>();

        // 遍历 source
        multiSource.keySet()
                .stream()
                .forEach(key ->{
            List<MerchandiseDomain> sourceList = (List<MerchandiseDomain>) multiSource.get(key);
            if (sourceList.size() == 1){
               replaceNew.add(sourceList.get(0));
            } else if (sourceList.size() >1){
               log.error("存在重复的 sourceList");
            }

            List<MerchandiseDomain> targetList = (List<MerchandiseDomain>) multiTarget.get(key);
            if (targetList.size() == 1){
                // log.info("目标 {} 修改为 {}",targetList,sourceList);
                replaceOld.add(targetList.get(0));
            }else if (targetList.size()>1){
                log.error("要覆盖的数据不唯一 {}",targetList);
            }

        }); // for end

        // 必须先删除旧数据
       int delCount = del(replaceOld);
       log.info("删除数量 {}",delCount);
        // 再更新
       int addCount = addAll(replaceNew);
       log.info("添加数量 {}",addCount);
        // 最后是备份表的处理
        //  可以手动操作删除备份表

    }

    // 分批量新增
    private int addAll(List<MerchandiseDomain> replaceNew){
        if (CollectionUtils.isNotEmpty(replaceNew)){
            List<List<MerchandiseDomain>> partition = Lists.partition(replaceNew,200);
            AtomicReference<Integer> add = new AtomicReference<>(0);
            partition.stream().forEach(
                    valList ->  {
                        add.set(add(valList));
                    }
            );
            return add.get();
        }
        return 0;
    }

    private int add(List<MerchandiseDomain> replaceNew){
        return merchandiseService.insertBatchMerchandiseList(replaceNew);
    }

    // 批量删除旧数据
    public int del(List<MerchandiseDomain> replaceOld){
        List<MerAndSupplierDTO> delList = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(replaceOld)){
           replaceOld.stream().forEach(
                   val ->{
                       MerAndSupplierDTO update = new MerAndSupplierDTO();
                       update.setMerchandiseCode(val.getMerchandiseCode());
                       update.setSupplierCode(val.getSupplierCode());
                       delList.add(update);
                   }
           );
        }
        log.info("删除的商品数据 {}",delList);
        return merchandiseService.deleteByMulti(delList);
    }

    // 保留分组的第一个元素  （最新日期的）
    public Set<MerchandiseDomain> getDataSet(String jsonFile){

        Multimap<String, MerchandiseDomain> multimap = getDataMultimap(jsonFile);

        // 最后日期的数据Set
        Set<MerchandiseDomain> sets = new HashSet<>();

        multimap.keySet().stream().forEach(
                strKey -> {
                    List<MerchandiseDomain> orderList = (List<MerchandiseDomain>) multimap.get(strKey);
                    // 因为之前根据时间倒序进行排序  所以取第一个值
                    MerchandiseDomain lastDate = orderList.get(0);
                    sets.add(lastDate);
                }
        );

        return sets;
    }

    // 使用Multimap 特性 获取数据的分组
    public  Multimap<String, MerchandiseDomain> getDataMultimap(String jsonFile) {
        String data1 = ResourceUtil.readUtf8Str(jsonFile);
        JSONArray array1 = JsonMapperUtil.readJSONToClass(data1, JSONArray.class);

        // guava Multimap
        Multimap<String, MerchandiseDomain> multimap = ArrayListMultimap.create();
        final boolean ignoreCase = true;
        List<MerchandiseDomain> merList = new ArrayList<>(array1.size());
        array1.stream().forEach(valM -> {
            MerchandiseDomain mer = new MerchandiseDomain();
            FieldCopyUtil.downLineObjPropertyCopyToHumpObj((Map) valM, mer, ignoreCase);
            merList.add(mer);

        });


        if (CollectionUtils.isNotEmpty(merList)) {
            merList.stream().sorted(
                    Comparator.comparing(MerchandiseDomain::getSyncDate).reversed()
            )
                    .collect(Collectors.toList())
                    .stream()
                    .forEach(valM -> {
                        // 拼接一个唯一的 key
                        String key = valM.getMerchandiseCode()
                                .concat(concatSign)
                                .concat(valM.getSupplierCode());
                        // 放入进去的时候就会自动根据key值分组
                        multimap.put(key, valM);
                    });
        }

        return multimap;
    }


}
