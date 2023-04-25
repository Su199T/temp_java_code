
SELECT
			S_MERCHANDISE_WARN.NEXTVAL, 		 -- 预警编号 -->
			SYSDATE, 							 -- 预警日期 -->
--        商品价格预警
	        'SPJGYJ', 							 -- 预警方式 -->
			finalz.id, 							 -- 商品原料关联公示网站原料编号 -->
--        z.price
			ROUND(finalz.old_price,2),   		 -- 商品进价 -->
--        z.month_avg_price
			ROUND(finalz.month_avg_price,2), 	 -- 最新行情价 -->
--         z.sy_price
			ROUND(finalz.sy_price,2), 			 -- 上月月均行情价 -->
--  如果有个值为 null 则默认是0
--       (  (z.sy_price - month_avg_price) / month_avg_price * z.purchase_price + z.purchase_price )  * z.input_count_proportion
			ROUND(finalz.new_input_cost,2), 	 -- 投入价试算 -->

--        z.price - z.input_cost +(  (  (z.sy_price -month_avg_price) /month_avg_price * z.purchase_price + z.purchase_price) * z.input_count_proportion)
			ROUND(merchandise_price_compute,2),  -- 进价试算 -->

			ROUND(merchandise_price_compute - NVL(finalz.old_price,0),2), -- 进价增长数值 -->

			ROUND(
			    (merchandise_price_compute - NVL(finalz.old_price,0))
			          /finalz.old_price*100,2
			    )  -- 进价增长百分比 -->

			FROM
--            finalz start 1depth
			(
			  SELECT
			  z.id,
			  z.material_code,
			  z.material_region_code,
			  z.merchandise_material_code,
			  z.price old_price,
			  z.purchase_price,
			  z.input_count_proportion,
			  z.input_cost,
			  z.month_avg_price,
			  z.sy_price,

			  (

			      DECODE(
			 month_avg_price, NULL,0,
			           ( NVL(z.sy_price,0) - month_avg_price ) / month_avg_price * NVL(z.purchase_price,0)
			          )

			          + NVL(z.purchase_price,0)
			      ) * NVL(z.input_count_proportion,0)  new_input_cost,  -- 投入价试算 -->

			         NVL( z.price,0 ) - NVL( z.input_cost,0 ) + NVL(
        	      (
        	          DECODE(
        	              month_avg_price,NULL,0,(
        	              NVL(z.sy_price,0)-month_avg_price)
        	                                         / month_avg_price
        	                                         * NVL(z.purchase_price,0)
        	              )
        	              + NVL( z.purchase_price,0 )
        	          ) * NVL( z.input_count_proportion,0 ),0
        	      ) merchandise_price_compute
			  FROM
-- 			  z start 2depth
			    (
			    SELECT
			    zmrm.id,
			    zmrm.material_code,
			    zmrm.merchandise_material_code,
			    zmrm.material_region_code,
--               商品采购价
			    jj.price,
--              (分组之后) 原料价格平均值
			    hq.month_avg_price,
--              (分组之后) 原料价格平均值
			    sy.sy_price,
--               采购价
			    ing.purchase_price,
--
			    NVL(ing.input_count_proportion,0) / 100 input_count_proportion,
--              投料表
			    ing.input_cost

			    FROM merchandise mr
-- 			                    3depth
			    INNER JOIN v_merchandise_last_oa lastoa ON lastoa.merchandise_code = mr.merchandise_code
			                                                   AND lastoa.supplier_code = mr.supplier_code
			    INNER JOIN analysis_reports_m arm ON arm.application_code = lastoa.application_code
			    INNER JOIN cost_analysis_merchandise cam ON cam.reports_code = arm.reports_code
			                                                    AND cam.merchandise_code = lastoa.oa_merchandise_code
			                                                    AND cam.supplier_code = lastoa.oa_supplier_code
			    INNER JOIN ingredient_item ii ON ii.ingredient_code = cam.accounting_code
			    INNER JOIN merchandise_relevance_material zmrm ON zmrm.merchandise_material_code = ii.material_code
			    INNER JOIN
-- 			     jj start 3depth
			    (
			        -- 商品进价 -->
			        SELECT
			          f1.material_code,
			          f1.material_region_code,
			          f1.merchandise_material_code,
-- 			               商品采购价
			          f2.price
			        FROM
-- 			             f1 start 4depth
			        (
			        SELECT
			        ii.ingredient_code,
			        n1.merchandise_material_code,
			        n1.material_code,
			        n1.material_region_code
			        FROM
			        (
			          --  预警类型是商品价格预警的商品原料与对应的公示网站原料 -->
			           SELECT
			           mrm.merchandise_material_code,
			           mrm.material_code,
			           mrm.material_region_code
			           FROM
			           merchandise_relevance_material mrm,
			           material_warn_config mwc
			           WHERE mrm.material_code = mwc.material_code
			           AND mwc.warn_type = 'SPJGYJ'
			        ) n1
			        INNER JOIN ingredient_item ii ON n1.merchandise_material_code = ii.material_code
			        )
			        f1
-- 			            f1 end

-- 			            f2 start  4depth
			        INNER JOIN
			        (
			          -- 最后一次OA申请的信息 -->
			          SELECT
			          oa.*,
			          cam.accounting_code,
			          mcp.price
			          FROM v_merchandise_last_oa oa
-- 			                     商品采购价
			          INNER JOIN merchandise_contract_price mcp
			              ON mcp.application_code = oa.application_code
			              AND mcp.merchandise_code = oa.oa_merchandise_code
			              AND mcp.supplier_code = oa.oa_supplier_code

			          INNER JOIN analysis_reports_m arm
			              ON arm.application_code = oa.application_code

			          INNER JOIN cost_analysis_merchandise cam
			              ON cam.reports_code = arm.reports_code
			              AND cam.merchandise_code = oa.oa_merchandise_code
			              AND cam.supplier_code = oa.oa_supplier_code

			          AND (mcp.region = '全国' OR mcp.region LIKE '%X001%')
			        )
			            f2
-- 			            f2 end 4depth
			            ON f1.ingredient_code = f2.accounting_code
                ) jj
--              jj end

			ON jj.material_code = zmrm.material_code
			       AND
			jj.merchandise_material_code = zmrm.merchandise_material_code
			       AND
			jj.material_region_code = zmrm.material_region_code

			INNER JOIN
-- 			        关联hq start
-- 			    hq  start 3depth
			    (

			        -- 公示网站原料行情原价 -->
			        SELECT
			        mp.material_code,
			        mp.material_region_code,
			        z1.merchandise_material_code,
			        AVG(NVL(mp.price,0)) month_avg_price
			        FROM material_price mp
-- 			          z1 start 4depth
			        INNER JOIN
			        (
			            SELECT
			            f1.material_code,
			            f1.material_region_code,
			            f1.merchandise_material_code,
			            f2.accounting_code,
			            f2.oa_approve_date
-- 			             f1 start 5depth
			            FROM
			            (
			                  SELECT
			                  ii.ingredient_code,
			                  n1.merchandise_material_code,
			                  n1.material_code,
			                  n1.material_region_code
			                  FROM
			                  (
			                     -- 预警类型是商品价格预警的商品原料与对应的公示网站原料 -->
			                     SELECT
			                     mrm.merchandise_material_code,
			                     mrm.material_code,
			                     mrm.material_region_code
			                     FROM
			                     merchandise_relevance_material mrm,
			                     material_warn_config mwc
			                     WHERE mrm.material_code = mwc.material_code
			                     AND mwc.warn_type = 'SPJGYJ'
			                  ) n1
			                  INNER JOIN ingredient_item ii ON n1.merchandise_material_code = ii.material_code
			            ) f1
-- 			                f1 end
-- 			               f2 start 5depth
			            INNER JOIN
			            (
			                  -- 最后一次OA申请的信息 -->
			                  SELECT
			                  oa.*,
			                  cam.accounting_code
			                  FROM v_merchandise_last_oa oa
			                  INNER JOIN analysis_reports_m arm ON arm.application_code = oa.application_code
			                  INNER JOIN cost_analysis_merchandise cam ON cam.reports_code = arm.reports_code AND cam.merchandise_code = oa.oa_merchandise_code AND cam.supplier_code = oa.oa_supplier_code

			            ) f2
-- 			                f2 end
			                ON f1.ingredient_code = f2.accounting_code
			        ) z1
-- 			            z1 end
			            ON z1.material_code = mp.material_code
			                   AND z1.material_region_code = mp.material_region_code
			        WHERE TO_CHAR(mp.price_date,'yyyymm') = TO_CHAR(z1.oa_approve_date,'yyyymm')
			        GROUP BY  mp.material_code ,
			                 mp.material_region_code ,
			                 merchandise_material_code
			    ) hq
--              hq end

			    ON hq.material_code = zmrm.material_code
			      AND hq.merchandise_material_code = zmrm.merchandise_material_code
			      AND hq.material_region_code = zmrm.material_region_code

			INNER JOIN
-- 			     sy start 3depth
			    (   -- 公示网站上月原料行情价 -->
			        SELECT
			          mrm.material_code,
			          mrm.material_region_code,
			          mrm.merchandise_material_code,
			          AVG( NVL(m.price,0) ) sy_price
-- 			        start 4depth
			        FROM
			        merchandise_relevance_material mrm,
			        material_warn_config mwc,
			        material_price m
			        WHERE mrm.material_code = mwc.material_code
			        AND m.material_code = mrm.material_code AND m.material_region_code = mrm.material_region_code
                            AND mwc.warn_type = 'SPJGYJ'
                        AND TO_CHAR(ADD_MONTHS(SYSDATE,-1),'yyyymm') = TO_CHAR(m.price_date,'yyyymm')
			        GROUP BY mrm.material_code ,
			                 mrm.material_region_code ,
			                 mrm.merchandise_material_code
			    ) sy
--              sy end

           ON sy.material_code = zmrm.material_code

                  AND sy.merchandise_material_code = zmrm.merchandise_material_code

           AND sy.material_region_code = zmrm.material_region_code

		   INNER JOIN
-- 			        ing start 3depth
			    (
			        -- 原料所在的投料表的采购价,投料占比,供应商原料投入价 -->
			        SELECT
			        f1.*
			        FROM
-- 			             f1 start 4depth
			        (
			            SELECT
			            n1.material_code ,
			            n1.material_region_code ,
			            n1.merchandise_material_code ,
			            ii.ingredient_code ,
			            ii.purchase_price ,
			            ii.input_count_proportion ,
			            ii.input_cost
			            FROM
			            (
			               SELECT
			               mrm.merchandise_material_code ,
			               mrm.material_code ,
			               mrm.material_region_code
			               FROM
			               merchandise_relevance_material mrm ,
			               material_warn_config mwc
			               WHERE mrm.material_code = mwc.material_code
			               AND mwc.warn_type = 'SPJGYJ'
			            ) n1
			            INNER JOIN ingredient_item ii
			                ON n1.merchandise_material_code = ii.material_code
			        )
			            f1
-- 			             f1 end

-- 			            f2 start 4depth
			        INNER JOIN
			        (
			            -- 最后一次OA申请的信息 -->
			            SELECT
			            oa.*,
			            cam.accounting_code
			            FROM v_merchandise_last_oa oa
			            INNER JOIN analysis_reports_m arm ON arm.application_code = oa.application_code
			            INNER JOIN cost_analysis_merchandise cam ON cam.reports_code = arm.reports_code
			            AND cam.merchandise_code = oa.oa_merchandise_code AND cam.supplier_code = oa.oa_supplier_code
			        ) f2
-- 			            f2 end
			            ON f1.ingredient_code = f2.accounting_code
			    ) ing
--                   ing end
			     ON ing.material_code = zmrm.material_code AND
			     ing.merchandise_material_code = zmrm.merchandise_material_code AND
			     ing.material_region_code = zmrm.material_region_code
			  ) z
-- 			    z end
			) finalz
--            finalz end