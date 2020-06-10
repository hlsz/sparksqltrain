/**
 * 通联数据机密
 * --------------------------------------------------------------------
 * 通联数据股份公司版权所有 © 2013-2020
 *
 * 注意：本文所载所有信息均属于通联数据股份公司资产。本文所包含的知识和技术概念均属于
 * 通联数据产权，并可能由中国、美国和其他国家专利或申请中的专利所覆盖，并受商业秘密或
 * 版权法保护。
 * 除非事先获得通联数据股份公司书面许可，严禁传播文中信息或复制本材料。
 *
 * DataYes CONFIDENTIAL
 * --------------------------------------------------------------------
 * Copyright © 2013-2020 DataYes, All Rights Reserved.
 *
 * NOTICE: All information contained herein is the property of DataYes
 * Incorporated. The intellectual and technical concepts contained herein are
 * proprietary to DataYes Incorporated, and may be covered by China, U.S. and
 * Other Countries Patents, patents in process, and are protected by trade
 * secret or copyright law.
 * Dissemination of this information or reproduction of this material is
 * strictly forbidden unless prior written permission is obtained from DataYes.
 */

package com.data.spark.sparksql.bdb.util

import com.datayes.bdb.bean.{ApplicationResult, ApplicationRow, ProfitBeanNew}
/**
  * Created by hongnan.li on 2017/8/6.
  */
object ProfitUtil {

  def calRateRecursive (a: Iterable[ProfitBeanNew]) : List[ProfitBeanNew] = {
    val list : List[ProfitBeanNew] = a.toList.sortBy(x => x.getDate)
    var status : Integer = 0
    var currentIndex : Integer = 0
    while (currentIndex < list.size) {

      val now: ProfitBeanNew = list(currentIndex)
      if (status == 0) {
        // 认为没有开始计算
        if (now.getTotalNetAsset == 0) {
          // 仍然没有持仓
          now.setNet(0)
          now.setRate(1)
        } else {
          // 有持仓，开始计算
          now.setNet(1)
          now.setShare(now.getTotalNetAsset)
          now.setRate(1)
          status = 1
        }
      } else {
        // 认为在计算过程当中
        if (now.getTotalNetAsset == 0) {
          // 认为清仓了
          status = 0
          now.setNet(0)
          now.setRate(1)
          now.setShare(0)
        } else {
          //　正常根据公式进行计算
          val preItem = list(currentIndex - 1)
          val share = now.getCapital / preItem.getNet + preItem.share

          now.setShare(share)
          if (share == 0) {
            //　如果份额为０，重置状态
            now.setNet(0)
            now.setRate(1)
            status = 0
          } else {
            val net = now.getTotalNetAsset / share
            now.setNet(net)
            now.setRate(net / preItem.getNet)

            if (now.getRate > 1.1 && now.getTotalNetAsset - preItem.getTotalNetAsset <= 100) {
              // 处理极小的利息问题
              //
              now.setRate(1)
            }
          }
        }
      }
      if (currentIndex == 0) {
        now.setDailyReturn(0d)
      } else {
        val preItem = list(currentIndex - 1)
        now.setDailyReturn((now.getTotalNetAsset - now.getCapital) - preItem.getTotalNetAsset)
      }
      currentIndex = currentIndex + 1
    }
    list
  }



  def calStockRateRecursive (a: Iterable[ProfitBeanNew]) : List[ProfitBeanNew] = {

    val list : List[ProfitBeanNew] = a.toList.sortBy(x => x.getDate)
    var status : Integer = 0
    var currentIndex : Integer = 0

    while (currentIndex < list.size) {
      val now: ProfitBeanNew = list(currentIndex)
      if (status == 0) {
        // 认为没有开始计算
        if (now.getTotalNetAsset == 0) {
          // 仍然没有持仓
          now.setNet(0)
          now.setRate(1)
        } else {
          // 有持仓，开始计算
          now.setNet(1)
          now.setShare(now.getTotalNetAsset)
          now.setRate(1)
          status = 1
        }
      } else {
        // 认为在计算过程当中
        if (now.getTotalNetAsset == 0) {
          // 认为清仓了
          status = 0
          now.setNet(0)
          now.setRate(1)
          now.setShare(0)
        } else {
          //　正常根据公式进行计算
          val preItem = list(currentIndex - 1)

          val share = now.getCapital / preItem.getNet + preItem.share
          now.setShare(share)
          if (share == 0) {
            now.setNet(0)
            now.setRate(1)
            status = 0
          } else if (share < 0 || share < (preItem.share * 0.7)) {
            // 认为是哪种一次性卖空了百分之99的股票的这种情况
            val tmpAsset = now.getTotalNetAsset - now.getCapital
            now.setNet(tmpAsset / preItem.share)
            now.setShare(now.getTotalNetAsset / tmpAsset * preItem.share)
            now.setRate((now.getTotalNetAsset - now.getCapital) / preItem.getTotalNetAsset)
          } else {
            val net = now.getTotalNetAsset / share
            now.setNet(net)
            now.setRate(net / preItem.getNet)

            if (now.getRate > 1.1 && now.getTotalNetAsset - preItem.getTotalNetAsset <= 100) {
              // 处理极小的利息问题
              now.setRate(1)
            }
          }
        }
      }

      if (currentIndex == 0) {
        now.setDailyReturn(0d)
      } else {
        val preItem = list(currentIndex - 1)
        now.setDailyReturn((now.getTotalNetAsset - now.getCapital) - preItem.getTotalNetAsset)
      }
      currentIndex = currentIndex + 1
    }
    list
  }

  def calFundRateRecursive (a: Iterable[ProfitBeanNew]) : List[ProfitBeanNew] = {
    val list : List[ProfitBeanNew] = a.toList.sortBy(x => x.getDate)
    var status : Integer = 0
    var currentIndex : Integer = 0

    while (currentIndex < list.size) {
      val now: ProfitBeanNew = list(currentIndex)
      if (status == 0) {
        // 认为没有开始计算
        if (now.getTotalNetAsset == 0) {
          // 仍然没有持仓
          now.setNet(0)
          now.setRate(1)
        } else {
          // 有持仓，开始计算
          now.setNet(1)
          now.setShare(now.getTotalNetAsset)
          now.setRate(1)
          status = 1
        }
      } else {
        // 认为在计算过程当中
        if (now.getTotalNetAsset == 0) {
          // 认为清仓了
          status = 0
          now.setNet(0)
          now.setRate(1)
          now.setShare(0)
        } else {
          //　正常根据公式进行计算
          val preItem = list(currentIndex - 1)

          val share = now.getCapital / preItem.getNet + preItem.share
          now.setShare(share)
          if (share == 0) {
            now.setNet(0)
            now.setRate(1)
            status = 0
          } else if (share < 0 || share < preItem.share) {
            // 认为是哪种一次性卖空了百分之99的股票的这种情况
            val tmpAsset = now.getTotalNetAsset - now.getCapital
            now.setNet(tmpAsset / preItem.share)
            now.setShare(now.getTotalNetAsset / tmpAsset * preItem.share)
            now.setRate((now.getTotalNetAsset - now.getCapital) / preItem.getTotalNetAsset)
          } else {
            val net = now.getTotalNetAsset / share
            now.setNet(net)
            now.setRate(net / preItem.getNet)

            if (now.getRate > 1.1 && now.getTotalNetAsset - preItem.getTotalNetAsset <= 100) {
              // 处理极小的利息问题
              now.setRate(1)
            }
          }
        }
      }

      if (currentIndex == 0) {
        now.setDailyReturn(0d)
      } else {
        val preItem = list(currentIndex - 1)
        now.setDailyReturn((now.getTotalNetAsset - now.getCapital) - preItem.getTotalNetAsset)
      }
      currentIndex = currentIndex + 1
    }
    list
  }




  def calManagementRateRecursive (a: Iterable[ProfitBeanNew]) : List[ProfitBeanNew] = {
    val list : List[ProfitBeanNew] = a.toList.sortBy(x => x.getDate)
    var status : Integer = 0
    var currentIndex : Integer = 0

    while (currentIndex < list.size) {
      val now: ProfitBeanNew = list(currentIndex)
      if (status == 0) {
        // 认为没有开始计算
        if (now.getTotalNetAsset == 0) {
          // 仍然没有持仓
          now.setNet(0)
          now.setRate(1)
        } else {
          // 有持仓，开始计算
          now.setNet(1)
          now.setShare(now.getTotalNetAsset)
          now.setRate(1)
          status = 1
        }
      } else {
        // 认为在计算过程当中
        if (now.getTotalNetAsset == 0) {
          // 认为清仓了
          status = 0
          now.setNet(0)
          now.setRate(1)
          now.setShare(0)
        } else {
          //　正常根据公式进行计算
          val preItem = list(currentIndex - 1)

          val share = now.getCapital / preItem.getNet + preItem.share
          now.setShare(share)
          if (share == 0) {
            now.setNet(0)
            now.setRate(1)
            status = 0
          } else if (share < 0 || share < preItem.share) {
            // 认为是哪种一次性卖空了百分之99的股票的这种情况
            val tmpAsset = now.getTotalNetAsset - now.getCapital
            now.setNet(tmpAsset / preItem.share)
            now.setShare(now.getTotalNetAsset / tmpAsset * preItem.share)
            now.setRate((now.getTotalNetAsset - now.getCapital) / preItem.getTotalNetAsset)
          } else {
            val net = now.getTotalNetAsset / share
            now.setNet(net)
            now.setRate(net / preItem.getNet)

            if (now.getRate > 1.1 && now.getTotalNetAsset - preItem.getTotalNetAsset <= 100) {
              // 处理极小的利息问题
              now.setRate(1)
            }
          }
        }
      }

      if (currentIndex == 0) {
        now.setDailyReturn(0d)
      } else {
        val preItem = list(currentIndex - 1)
        now.setDailyReturn((now.getTotalNetAsset - now.getCapital) - preItem.getTotalNetAsset)
      }

      currentIndex = currentIndex + 1
    }
    list
  }


  def calBondRateRecursive (a: Iterable[ProfitBeanNew]) : List[ProfitBeanNew] = {
    val list : List[ProfitBeanNew] = a.toList.sortBy(x => x.getDate)
    var status : Integer = 0
    var currentIndex : Integer = 0
    while (currentIndex < list.size) {
      val now: ProfitBeanNew = list(currentIndex)
      if (status == 0) {
        // 认为没有开始计算
        if (now.getTotalNetAsset == 0) {
          // 仍然没有持仓
          now.setNet(0)
          now.setRate(1)
        } else {
          // 有持仓，开始计算
          now.setNet(1)
          now.setShare(now.getTotalNetAsset)
          now.setRate(1)
          status = 1
        }
      } else {
        // 认为在计算过程当中
        if (now.getTotalNetAsset == 0) {
          // 认为清仓了
          status = 0
          now.setNet(0)
          now.setRate(1)
          now.setShare(0)
        } else {
          //　正常根据公式进行计算
          val preItem = list(currentIndex - 1)

          val share = now.getCapital / preItem.getNet + preItem.share
          now.setShare(share)
          if (share == 0) {
            now.setNet(0)
            now.setRate(1)
            status = 0
          } else if (share < 0 || share < (preItem.share * 0.7)) {
            // 认为是哪种一次性卖空了百分之99的股票的这种情况
            val tmpAsset = now.getTotalNetAsset - now.getCapital
            now.setNet(tmpAsset / preItem.share)
            now.setShare(now.getTotalNetAsset / tmpAsset * preItem.share)
            now.setRate((now.getTotalNetAsset - now.getCapital) / preItem.getTotalNetAsset)

          } else {
            val net = now.getTotalNetAsset / share
            now.setNet(net)
            now.setRate(net / preItem.getNet)

            if (now.getRate > 1.1 && now.getTotalNetAsset - preItem.getTotalNetAsset <= 100) {
              // 处理极小的利息问题
              now.setRate(1)
            }
          }
        }
      }

      if (currentIndex == 0) {
        now.setDailyReturn(0d)
      } else {
        val preItem = list(currentIndex - 1)
        now.setDailyReturn((now.getTotalNetAsset - now.getCapital) - preItem.getTotalNetAsset)
      }
      currentIndex = currentIndex + 1
    }
    list
  }


  /**
    * 逆回购计算收益
    * @return
    */
  def calRepoRateRecursive (a: Iterable[ProfitBeanNew]) : List[ProfitBeanNew] = {

    val list : List[ProfitBeanNew] = a.toList.sortBy(x => x.getDate)
    var status : Integer = 0
    var currentIndex : Integer = 0

    while (currentIndex < list.size) {

      val now: ProfitBeanNew = list(currentIndex)
      if (status == 0) {
        // 认为没有开始计算
        if (now.getTotalNetAsset == 0) {
          // 仍然没有持仓
          now.setNet(0)
          now.setRate(1)
        } else {
          // 有持仓，开始计算
          now.setNet(1)
          now.setShare(now.getTotalNetAsset)
          now.setRate(1)
          status = 1
        }
      } else {
        // 认为在计算过程当中
        if (now.getTotalNetAsset == 0) {
          // 认为清仓了
          status = 0
          now.setNet(0)
          now.setRate(1)
          now.setShare(0)
        } else {
          //　正常根据公式进行计算
          val preItem = list(currentIndex - 1)

          val share = now.getCapital / preItem.getNet + preItem.share
          now.setShare(share)
          if (share == 0) {
            now.setNet(0)
            now.setRate(1)
            status = 0
          } else if (share < 0 || share < (preItem.share * 0.7)) {
            // 认为是哪种一次性卖空了百分之99的股票的这种情况
            val tmpAsset = now.getTotalNetAsset - now.getCapital
            now.setNet(tmpAsset / preItem.share)
            now.setShare(now.getTotalNetAsset / tmpAsset * preItem.share)
            now.setRate((now.getTotalNetAsset - now.getCapital) / preItem.getTotalNetAsset)

          } else {
            val net = now.getTotalNetAsset / share
            now.setNet(net)
            now.setRate(net / preItem.getNet)

            if (now.getRate > 1.1 && now.getTotalNetAsset - preItem.getTotalNetAsset <= 100) {
              // 处理极小的利息问题
              now.setRate(1)
            }
          }
        }
      }

      if (currentIndex == 0) {
        now.setDailyReturn(0d)
      } else {
        val preItem = list(currentIndex - 1)
        now.setDailyReturn((now.getTotalNetAsset - now.getCapital) - preItem.getTotalNetAsset)
      }
      currentIndex = currentIndex + 1
    }
    list
  }



  def calApplicationProfit (a: Iterable[ApplicationRow]) : ApplicationResult = {
    val list : List[ApplicationRow] = a.toList.sortBy(x => x.getDate)

    var leftNum = list(0).getTotalNum
    val userID = list(0).getUserID
    val stockID = list(0).getStockID
    var gain = 0.0
    var cost = list(0).getCost

    var currentIndex : Integer = 0
    while (currentIndex < list.size && leftNum > 0) {
      val row : ApplicationRow = list(currentIndex)

      if (row.getNum < leftNum) {
        gain = gain + row.getNum * row.getPrice
        leftNum = leftNum - row.getNum
      } else {
        gain = gain + leftNum * row.getPrice
        leftNum = 0
      }

      currentIndex = currentIndex + 1
    }

    new ApplicationResult(userID, stockID, gain - cost, leftNum)
  }


}
