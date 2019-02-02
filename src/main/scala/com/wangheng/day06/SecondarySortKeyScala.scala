package com.wangheng.day06

class SecondarySortKeyScala(val first: Int, val second: Int)
  extends Ordered[SecondarySortKeyScala] with Serializable {
  def compare(that: SecondarySortKeyScala): Int = {
    if(this.first - that.first != 0){
      this.first - that.first
    }else{
      this.second - that.second
    }
  }
}
