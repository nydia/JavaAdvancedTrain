package io.kimmking.cache.Service;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import lombok.SneakyThrows;

/**
 * @Description TODO
 * @Date 2022/1/6 12:49
 * @Created by <a href="mailto:nydia_lhq@hotmail.com">lvhuaqiang</a>
 */
public class BloomFilterGuava {

    /**
     <dependency>
     <groupId>com.google.guava</groupId>
     <artifactId>guava</artifactId>
     <version>23.0</version>
     </dependency>
     * @param args
     */
    @SneakyThrows
    public static void main(String[] args) {
        //后边两个参数：预计包含的数据量，和允许的误差值
        BloomFilter<Integer> bloomFilter = BloomFilter.create(Funnels.integerFunnel(), 100000, 0.01);
        for (int i = 0; i < 100000; i++) {
            bloomFilter.put(i);
        }
        System.out.println(bloomFilter.mightContain(1));
        System.out.println(bloomFilter.mightContain(2));
        System.out.println(bloomFilter.mightContain(3));
        System.out.println(bloomFilter.mightContain(100001));

        //bloomFilter.writeTo();
    }
}
