package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"

	"github.com/CocaineCong/gin-mall/consts"
	"github.com/CocaineCong/gin-mall/pkg/utils/ctl"
	"github.com/CocaineCong/gin-mall/pkg/utils/log"
	"github.com/CocaineCong/gin-mall/repository/cache"
	"github.com/CocaineCong/gin-mall/repository/db/dao"
	"github.com/CocaineCong/gin-mall/repository/db/model"
	"github.com/CocaineCong/gin-mall/repository/kafka"
	"github.com/CocaineCong/gin-mall/types"
)

var SkillProductSrvIns *SkillProductSrv
var SkillProductSrvOnce sync.Once

type SkillProductSrv struct {
}

func GetSkillProductSrv() *SkillProductSrv {
	SkillProductSrvOnce.Do(func() {
		SkillProductSrvIns = &SkillProductSrv{}
	})
	return SkillProductSrvIns
}

// InitSkillGoods 初始化商品信息
func (s *SkillProductSrv) InitSkillGoods(ctx context.Context) (resp interface{}, err error) {
	spList := make([]*model.SkillProduct, 0)
	for i := 1; i < 10; i++ {
		spList = append(spList, &model.SkillProduct{
			ProductId: uint(i),
			BossId:    2,
			Title:     "秒杀商品测试使用",
			Money:     200,
			Num:       10,
		})
	}
	err = dao.NewSkillGoodsDao(ctx).BatchCreate(spList)
	if err != nil {
		log.LogrusObj.Infoln(err)
		return
	}

	// 导入数据库的同时，初始化缓存
	rc := cache.RedisClient
	_, _ = rc.Del(ctx, cache.SkillProductListKey).Result()
	for i := range spList {
		jsonBytes, errx := json.Marshal(spList[i])
		if errx != nil {
			log.LogrusObj.Infoln(errx)
			return
		}
		jsonString := string(jsonBytes)
		_, errx = rc.LPush(ctx, cache.SkillProductListKey, jsonString).Result()
		if errx != nil {
			log.LogrusObj.Infoln(errx)
			return nil, errx
		}

		errx = rc.Set(ctx, fmt.Sprintf(cache.SkillProductKey, spList[i].ProductId), jsonString, 0).Err()
		if errx != nil {
			log.LogrusObj.Infoln(errx)
			return nil, errx
		}

		errx = rc.Set(ctx, cache.SkillStockKeyByProductId(spList[i].ProductId), spList[i].Num, 0).Err()
		if errx != nil {
			log.LogrusObj.Infoln(errx)
			return nil, errx
		}
	}

	return
}

// ListSkillGoods 列表展示
func (s *SkillProductSrv) ListSkillGoods(ctx context.Context) (resp interface{}, err error) {
	// 读缓存
	rc := cache.RedisClient
	// 获取列表
	skillProductList, err := rc.LRange(ctx, cache.SkillProductListKey, 0, -1).Result()
	if err != nil {
		log.LogrusObj.Infoln(err)
		return
	}

	if len(skillProductList) == 0 {
		skill, errx := dao.NewSkillGoodsDao(ctx).ListSkillGoods()
		if errx != nil {
			log.LogrusObj.Infoln(errx)
			return nil, errx
		}

		for i := range skill {
			// 将结构体转换为JSON格式的字符串
			jsonBytes, errx := json.Marshal(skill[i])
			if errx != nil {
				log.LogrusObj.Infoln(errx)
				return
			}
			// 将字节数组转换为字符串
			jsonString := string(jsonBytes)
			_, errx = rc.LPush(ctx, cache.SkillProductListKey, jsonString).Result()
			if errx != nil {
				log.LogrusObj.Infoln(errx)
				return nil, errx
			}
		}
		resp = skill
	} else {
		resp = skillProductList
	}

	return
}

// GetSkillGoods 详情展示
func (s *SkillProductSrv) GetSkillGoods(ctx context.Context, req *types.GetSkillProductReq) (resp interface{}, err error) {
	// 读缓存
	rc := cache.RedisClient
	// 获取列表
	resp, err = rc.Get(ctx,
		fmt.Sprintf(cache.SkillProductKey, req.ProductId)).Result()
	if err != nil {
		log.LogrusObj.Infoln(err)
		return
	}

	return
}

// SkillProduct 秒杀商品
func (s *SkillProductSrv) SkillProduct(ctx context.Context, req *types.SkillProductReq) (resp interface{}, err error) {
	u, err := ctl.GetUserInfo(ctx)
	if err != nil {
		log.LogrusObj.Infoln(err)
		return nil, err
	}

	rc := cache.RedisClient
	detailJSON, err := rc.Get(ctx, fmt.Sprintf(cache.SkillProductKey, req.ProductId)).Result()
	if err != nil {
		log.LogrusObj.Infoln(err)
		return nil, err
	}

	sp := new(model.SkillProduct)
	err = json.Unmarshal([]byte(detailJSON), sp)
	if err != nil {
		log.LogrusObj.Infoln(err)
		return nil, err
	}

	stockKey := cache.SkillStockKeyByProductId(req.ProductId)
	userKey := cache.SkillUserKey(u.Id, req.ProductId)

	ret, err := rc.Eval(ctx, skillProductLua, []string{stockKey, userKey}, 86400).Int()
	if err != nil {
		log.LogrusObj.Infoln(err)
		return nil, err
	}
	if ret == 0 {
		return nil, errors.New("已售罄")
	}
	if ret == -1 {
		return nil, errors.New("重复抢购")
	}

	orderNum := genOrderNum(req.ProductId, u.Id)
	msg := &model.SkillProduct2MQ{
		SkillProductId: req.SkillProductId,
		ProductId:      req.ProductId,
		BossId:         sp.BossId,
		UserId:         u.Id,
		Money:          sp.Money,
		AddressId:      req.AddressId,
		Key:            req.Key,
		OrderNum:       orderNum,
		Num:            1,
	}

	msgBytes, err := json.Marshal(msg)
	if err != nil {
		log.LogrusObj.Infoln(err)
		return nil, err
	}

	kafkaKey := kafka.DefaultKey()
	if kafkaKey == "" {
		_, _ = rc.Incr(ctx, stockKey).Result()
		_, _ = rc.Del(ctx, userKey).Result()
		return nil, errors.New("kafka 未配置")
	}
	err = kafka.SendMessage(ctx, kafkaKey, consts.SkillOrderTopic, string(msgBytes))
	if err != nil {
		_, _ = rc.Incr(ctx, stockKey).Result()
		_, _ = rc.Del(ctx, userKey).Result()
		log.LogrusObj.Infoln(err)
		return nil, err
	}

	resp = &types.SkillPurchaseResp{OrderNum: orderNum}
	return
}

func RunSkillOrderConsumer(ctx context.Context) error {
	kafkaKey := kafka.DefaultKey()
	if kafkaKey == "" {
		return errors.New("kafka 未配置")
	}

	return kafka.ConsumerGroup(ctx, kafkaKey, consts.SkillOrderGroupID, consts.SkillOrderTopic, kafka.ConsumerGroupHandler(func(message *sarama.ConsumerMessage) error {
		req := new(model.SkillProduct2MQ)
		err := json.Unmarshal(message.Value, req)
		if err != nil {
			return err
		}

		orderDao := dao.NewOrderDao(ctx)
		_, err = orderDao.GetOrderByOrderNum(req.OrderNum)
		if err == nil {
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		order := &model.Order{
			UserID:    req.UserId,
			ProductID: req.ProductId,
			BossID:    req.BossId,
			AddressID: req.AddressId,
			Num:       req.Num,
			OrderNum:  req.OrderNum,
			Type:      1,
			Money:     req.Money,
		}

		err = orderDao.CreateOrder(order)
		if err != nil {
			return err
		}

		data := redis.Z{
			Score:  float64(time.Now().Unix()) + 15*time.Minute.Seconds(),
			Member: req.OrderNum,
		}
		cache.RedisClient.ZAdd(cache.RedisContext, OrderTimeKey, data)

		return nil
	}))
}

const skillProductLua = `
local stock = tonumber(redis.call('GET', KEYS[1]) or '0')
if stock <= 0 then
  return 0
end
if redis.call('EXISTS', KEYS[2]) == 1 then
  return -1
end
redis.call('DECR', KEYS[1])
redis.call('SET', KEYS[2], '1', 'EX', tonumber(ARGV[1]))
return 1
`

func genOrderNum(productId, userId uint) uint64 {
	number := fmt.Sprintf("%09v", rand.New(rand.NewSource(time.Now().UnixNano())).Int31n(1000000000))
	number = number + strconv.Itoa(int(productId)) + strconv.Itoa(int(userId))
	orderNum, _ := strconv.ParseUint(number, 10, 64)
	return orderNum
}

// SkillProductMQ2MySQL 从mq落库
// func SkillProductMQ2MySQL(ctx context.Context, req *story_types.LikeStoryReq) (err error) {
// 	storyDao := dao.NewStoryDao(ctx)
// 	usDao := dao.NewUserStoryDao(ctx)
// 	err = storyDao.UpdateStoryLikeOrStar(req.StoryId, 1, false)
// 	if err != nil {
// 		log.LogrusObj.Infoln(err)
// 		return
// 	}
//
// 	err = usDao.UserStoryUpsert(&user_story_types.UserStoryReq{
// 		UserId:        req.UserId,
// 		StoryId:       req.StoryId,
// 		OperationType: user_story_consts.UserStoryOperationTypeLike,
// 	})
// 	if err != nil {
// 		log.LogrusObj.Infoln(err)
// 		return
// 	}
//
// 	return
// }
