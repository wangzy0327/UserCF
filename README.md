# UserCF

## MapReduce步骤(UserCF)

### 1、根据用户行为列表构建评分矩阵。
        输入：用户ID，物品ID，分值
        输出：用户ID(行)——物品ID(列)——分值
### 2、利用评分矩阵，构建用户与用户的相似度矩阵。
        输入：步骤1的输出
        缓存：步骤1的输出
        (输出和缓存是相同的文件)
        输出：用户ID(行)——用户ID(列)——相似度
### 3、将评分矩阵转置
        输入：步骤1的输出
        输出：物品ID(行)——用户ID(列)——分值
### 4、用户与用户相似度矩阵 x  评分矩阵(经过步骤3转置)
        输入：步骤2的输出
        缓存：步骤3的输出
        输出：用户ID(行)——物品ID(列)——分值
### 5、根据评分矩阵，将步骤4的输出中，用户已经有过行为的商品评分置0
        输入：步骤4的输出
        缓存：步骤1的输出
        输出：用户ID(行)——物品ID(列)——分值(最终的推荐列表)