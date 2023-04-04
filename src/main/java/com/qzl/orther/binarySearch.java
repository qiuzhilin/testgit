package com.qzl.orther;

public class binarySearch {
    //设定左右指针
    //找出中间位置，并判断该位置值是否等于 target
    //nums[mid] == target 则返回该位置下标
    //nums[mid] > target 则右侧指针移到中间
    //nums[mid] < target 则左侧指针移到中间

    public static void main(String[] args) {
        int[] nums={-3,2,4,7,9,11,23,45,67};
        System.out.println(binaryMethod(nums,9));
    }
    public static int binaryMethod(int[] ns, int target){
        //定义开始游标和结束游标
        int startc=0,endc=ns.length-1;
        //最小游标小于最大游标的情况下
        while(startc<=endc){
            //对比中间值
            int middle=(startc+endc)/2;
            if(target==ns[middle]){
                return middle;
            }else if(target>ns[middle]){
                startc=middle+1;
            }else {
                endc=middle-1;
            }
        }
        return -1;
    }
    //不存在便插入
    public int searchInsert(int[] nums, int target) {
        return 1;
    }

}
