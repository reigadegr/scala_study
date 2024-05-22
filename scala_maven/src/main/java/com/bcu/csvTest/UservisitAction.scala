package com.bcu.csvTest
//先按照点击数排名，靠前的就排名高;如果点击数相同，再比较下单数:下单数再相同，就比较支付数。
case class UservisitAction(
                            date: String,
                            user_id: Long,
                            session_id: String,
                            page_id: Long,
                            action_time: String,
                            search_keyword: String,
                            click_category_id: Long,
                            click_product_id: Long,
                            order_category_ids: String,
                            order_product_ids: String,
                            pay_category_ids: String,
                            pay_product_ids: String,
                            city_id: Long
                          )
