package index

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xssnick/tonutils-go/address"
)

// address utils
func getAccountAddressFriendly(account string, code_hash *string, account_status *string, is_testnet bool) string {
	addr, err := address.ParseRawAddr(strings.Trim(account, " "))
	if err != nil {
		return "addr_none"
	}
	bouncable := true
	if code_hash == nil {
		bouncable = false
	}
	if account_status != nil && *account_status == "uninit" {
		bouncable = false
	}
	if code_hash != nil && WalletsHashMap[*code_hash] {
		bouncable = false
	}
	addr.SetBounce(bouncable)
	addr.SetTestnetOnly(is_testnet)
	return addr.String()
}

// query builders
func limitQuery(lim LimitRequest, settings RequestSettings) (string, error) {
	query := ``
	if lim.Limit == nil {
		// set default value
		lim.Limit = new(int32)
		*lim.Limit = int32(settings.DefaultLimit)
	}
	if lim.Limit != nil {
		limit := max(1, *lim.Limit)
		if limit > int32(settings.MaxLimit) {
			return "", IndexError{Code: 422, Message: fmt.Sprintf("limit is not allowed: %d > %d", limit, settings.MaxLimit)}
		}
		query += fmt.Sprintf(" limit %d", limit)
	}
	if lim.Offset != nil {
		offset := max(0, *lim.Offset)
		query += fmt.Sprintf(" offset %d", offset)
	}
	return query, nil
}

func buildBlocksQuery(
	blk_req BlockRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) (string, error) {
	query := `select blocks.* from blocks`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	// filters
	if v := blk_req.Workchain; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("workchain = %d", *v))
	}
	if v := blk_req.Shard; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("shard = %d", *v))
	}
	if v := blk_req.Seqno; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("seqno = %d", *v))
	}
	if v := blk_req.McSeqno; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("mc_block_seqno = %d", *v))
	}

	order_col := "gen_utime"
	if v := utime_req.StartUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("gen_utime >= %d", *v))
	}
	if v := utime_req.EndUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("gen_utime <= %d", *v))
	}
	if v := lt_req.StartLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("start_lt >= %d", *v))
	}
	if v := lt_req.EndLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("start_lt <= %d", *v))
	}
	if v := lim_req.Sort; v != nil {
		orderby_query = fmt.Sprintf(" order by %s %s", order_col, *v)
	}

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}

	query += filter_query
	query += orderby_query
	query += limit_query
	return query, nil
}

func buildTransactionsQuery(
	blk_req BlockRequest,
	tx_req TransactionRequest,
	msg_req MessageRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) (string, error) {
	query := `select T.* from`
	from_query := ` transactions as T`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	sort_order := `desc`
	if lim_req.Sort != nil {
		sort_order = string(*lim_req.Sort)
	}

	// filters
	order_by_now := false
	if v := utime_req.StartUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.now >= %d", *v))
		order_by_now = true
	}
	if v := utime_req.EndUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.now <= %d", *v))
		order_by_now = true
	}
	if v := lt_req.StartLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.lt >= %d", *v))
	}
	if v := lt_req.EndLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.lt <= %d", *v))
	}
	if order_by_now {
		orderby_query = fmt.Sprintf(" order by T.now %s, T.lt %s, account asc", sort_order, sort_order)
	} else {
		orderby_query = fmt.Sprintf(" order by T.lt %s, account asc", sort_order)
	}

	if v := blk_req.Workchain; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.block_workchain = %d", *v))
	}
	if v := blk_req.Shard; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.block_shard = %d", *v))
	}
	if v := blk_req.Seqno; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.block_seqno = %d", *v))
	}
	if v := blk_req.McSeqno; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.mc_block_seqno = %d", *v))
	}

	if v := tx_req.Account; v != nil {
		if len(v) == 1 {
			filter_list = append(filter_list, fmt.Sprintf("T.account = '%s'", v[0]))
			if order_by_now {
				orderby_query = fmt.Sprintf(" order by account asc, T.now %s, T.lt %s", sort_order, sort_order)
			} else {
				orderby_query = fmt.Sprintf(" order by account asc, T.lt %s", sort_order)
			}
		} else if len(v) > 1 {
			vv := []string{}
			for _, x := range v {
				if len(x) > 0 {
					vv = append(vv, fmt.Sprintf("'%s'", x))
				}
			}
			vv_str := strings.Join(vv, ",")
			filter_list = append(filter_list, fmt.Sprintf("T.account in (%s)", vv_str))
		}
	}
	if v := tx_req.Hash; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.hash = '%s'", *v))
		orderby_query = ``
	}
	if v := tx_req.Lt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.lt = %d", *v))
		orderby_query = fmt.Sprintf(" order by T.lt, account %s", sort_order)
	}

	// transaction by message
	by_msg := false
	if v := msg_req.Direction; v != nil {
		by_msg = true
		filter_list = append(filter_list, fmt.Sprintf("M.direction = '%s'", *v))
	}
	if v := msg_req.MessageHash; v != nil {
		by_msg = true
		if len(v) == 1 {
			filter_list = append(filter_list, fmt.Sprintf("M.msg_hash = '%s'", v[0]))
		} else if len(v) > 1 {
			vv := []string{}
			for _, x := range v {
				if len(x) > 0 {
					vv = append(vv, fmt.Sprintf("'%s'", x))
				}
			}
			vv_str := strings.Join(vv, ",")
			filter_list = append(filter_list, fmt.Sprintf("M.msg_hash in (%s)", vv_str))
		}
	}
	if v := msg_req.Source; v != nil {
		by_msg = true
		filter_list = append(filter_list, fmt.Sprintf("M.source = '%s'", *v))
	}
	if v := msg_req.Destination; v != nil {
		by_msg = true
		filter_list = append(filter_list, fmt.Sprintf("M.destination = '%s'", *v))
	}
	if v := msg_req.BodyHash; v != nil {
		by_msg = true
		filter_list = append(filter_list, fmt.Sprintf("M.body_hash = '%s'", *v))
	}
	if v := msg_req.Opcode; v != nil {
		by_msg = true
		filter_list = append(filter_list, fmt.Sprintf("M.opcode = %d and M.direction = 'in'", *v))
	}
	if by_msg {
		from_query = " messages as M join transactions as T on M.tx_hash = T.hash and M.tx_lt = T.lt"
	}

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query += from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	// log.Println(query) // TODO: remove debug
	return query, nil
}

func buildMessagesQuery(
	msg_req MessageRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) (string, error) {
	all_columns := ` M.*, B.*, I.*`
	clmn_query := ` distinct on (M.msg_hash)` + all_columns
	from_query := ` messages as M 
		left join message_contents as B on M.body_hash = B.hash 
		left join message_contents as I on M.init_state_hash = I.hash`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	if v := msg_req.Direction; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.direction = '%s'", *v))
		clmn_query = all_columns
	}
	if v := msg_req.Source; v != nil {
		if *v == "null" {
			filter_list = append(filter_list, "M.source is NULL")
		} else {
			filter_list = append(filter_list, fmt.Sprintf("M.source = '%s'", *v))
		}
	}
	if v := msg_req.Destination; v != nil {
		if *v == "null" {
			filter_list = append(filter_list, "M.destination is NULL")
		} else {
			filter_list = append(filter_list, fmt.Sprintf("M.destination = '%s'", *v))
		}
	}
	if v := msg_req.Opcode; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.opcode = %d", *v))
	}
	if v := msg_req.MessageHash; v != nil {
		if len(v) == 1 {
			filter_list = append(filter_list, fmt.Sprintf("M.msg_hash = '%s'", v[0]))
		} else if len(v) > 1 {
			vv := []string{}
			for _, x := range v {
				if len(x) > 0 {
					vv = append(vv, fmt.Sprintf("'%s'", x))
				}
			}
			vv_str := strings.Join(vv, ",")
			filter_list = append(filter_list, fmt.Sprintf("M.msg_hash in (%s)", vv_str))
		}
	}
	if v := msg_req.BodyHash; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.body_hash = '%s'", *v))
	}

	order_col := "M.created_lt"
	if v := utime_req.StartUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.created_at >= %d", *v))
		order_col = "M.created_at"
	}
	if v := utime_req.EndUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.created_at <= %d", *v))
		order_col = "M.created_at"
	}
	if v := lt_req.StartLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.created_lt >= %d", *v))
	}
	if v := lt_req.EndLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("M.created_lt <= %d", *v))
	}
	if lim_req.Sort != nil {
		orderby_query = fmt.Sprintf(" order by %s %s", order_col, *lim_req.Sort)
	}

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query := `select` + clmn_query
	query += ` from ` + from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	// log.Println(query) // TODO: remove debug
	return query, nil
}

func filterByArray[T any](clmn string, values []T) string {
	filter_list := []string{}
	for _, x := range values {
		t := reflect.ValueOf(x)
		switch t.Kind() {
		case reflect.String:
			if t.Len() > 0 {
				filter_list = append(filter_list, fmt.Sprintf("'%s'", t.String()))
			}
		default:
			filter_list = append(filter_list, fmt.Sprintf("'%v'", x))
		}
	}
	if len(filter_list) == 1 {
		return fmt.Sprintf("%s = %s", clmn, filter_list[0])
	}
	if len(filter_list) > 1 {
		vv_str := strings.Join(filter_list, ",")
		return fmt.Sprintf("%s in (%s)", clmn, vv_str)
	}
	return ``
}

func buildNFTCollectionsQuery(nft_req NFTCollectionRequest, lim_req LimitRequest, settings RequestSettings) (string, error) {
	clmn_query := ` N.address, N.next_item_index, N.owner_address, N.collection_content, 
				    N.data_hash, N.code_hash, N.last_transaction_lt`
	from_query := ` nft_collections as N`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ` order by id asc`
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	if v := nft_req.CollectionAddress; v != nil {
		filter_str := filterByArray("N.address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		orderby_query = ``
	}
	if v := nft_req.OwnerAddress; v != nil {
		filter_str := filterByArray("N.owner_address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		orderby_query = ``
	}

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query := `select` + clmn_query
	query += ` from ` + from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	return query, nil
}

func buildNFTItemsQuery(nft_req NFTItemRequest, lim_req LimitRequest, settings RequestSettings) (string, error) {
	clmn_query := ` N.address, N.init, N.index, N.collection_address, N.owner_address, N.content, 
					N.last_transaction_lt, N.code_hash, N.data_hash,
					C.address, C.next_item_index, C.owner_address, C.collection_content, 
				    C.data_hash, C.code_hash, C.last_transaction_lt`
	from_query := ` nft_items as N left join nft_collections as C on N.collection_address = C.address`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ` order by N.id asc`
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	if v := nft_req.Address; v != nil {
		filter_str := filterByArray("N.address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		orderby_query = ``
	}
	if v := nft_req.OwnerAddress; v != nil {
		filter_str := filterByArray("N.owner_address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		orderby_query = ` order by N.owner_address, N.collection_address, N.index`
	}
	if v := nft_req.CollectionAddress; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("N.collection_address = '%s'", *v))
		orderby_query = ` order by collection_address, index`
	}
	if v := nft_req.Index; v != nil {
		if nft_req.CollectionAddress == nil {
			return ``, IndexError{Code: 422, Message: "index parameter is not allowed without the collection_address"}
		}
		filter_str := filterByArray("N.index", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
	}

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query := `select` + clmn_query
	query += ` from ` + from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	return query, nil
}

func buildNFTTransfersQuery(transfer_req NFTTransferRequest, utime_req UtimeRequest,
	lt_req LtRequest, lim_req LimitRequest, settings RequestSettings) (string, error) {
	clmn_query := ` T.*`
	from_query := ` nft_transfers as T`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	if v := transfer_req.OwnerAddress; v != nil {
		if v1 := transfer_req.Direction; v1 != nil {
			f_str := ``
			if *v1 == "in" {
				f_str = filterByArray("T.new_owner", v)
			} else {
				f_str = filterByArray("T.old_owner", v)
			}
			if len(f_str) > 0 {
				filter_list = append(filter_list, f_str)
			}
		} else {
			f1_str := filterByArray("T.old_owner", v)
			f2_str := filterByArray("T.new_owner", v)
			if len(f1_str) > 0 {
				filter_list = append(filter_list, fmt.Sprintf("(%s or %s)", f1_str, f2_str))
			}
		}
	}
	if v := transfer_req.ItemAddress; v != nil {
		filter_str := filterByArray("T.nft_item_address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
	}
	if v := transfer_req.CollectionAddress; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.nft_collection_address = '%s'", *v))
	}

	order_col := "T.tx_lt"
	if v := utime_req.StartUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_now >= %d", *v))
		order_col = "T.tx_now"
	}
	if v := utime_req.EndUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_now <= %d", *v))
		order_col = "T.tx_now"
	}
	if v := lt_req.StartLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_lt >= %d", *v))
	}
	if v := lt_req.EndLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_lt <= %d", *v))
	}
	if lim_req.Sort == nil {
		lim_req.Sort = new(SortType)
		*lim_req.Sort = "desc"
	}
	if lim_req.Sort != nil {
		orderby_query = fmt.Sprintf(" order by %s %s", order_col, *lim_req.Sort)
	}

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query := `select` + clmn_query
	query += ` from ` + from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	// log.Println(query)
	return query, nil
}

func buildJettonMastersQuery(jetton_req JettonMasterRequest, lim_req LimitRequest, settings RequestSettings) (string, error) {
	clmn_query := ` J.address, J.total_supply, J.mintable, J.admin_address, J.jetton_content, 
		J.jetton_wallet_code_hash, J.code_hash, J.data_hash, J.last_transaction_lt`
	from_query := ` jetton_masters as J`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ` order by id asc`
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	if v := jetton_req.MasterAddress; v != nil {
		filter_str := filterByArray("J.address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		orderby_query = ``
	}
	if v := jetton_req.AdminAddress; v != nil {
		filter_str := filterByArray("J.admin_address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
	}

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query := `select` + clmn_query
	query += ` from ` + from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	return query, nil
}

func buildJettonWalletsQuery(jetton_req JettonWalletRequest, lim_req LimitRequest, settings RequestSettings) (string, error) {
	clmn_query := `J.address, J.balance, J.owner, J.jetton, J.last_transaction_lt, J.code_hash, J.data_hash`
	from_query := `jetton_wallets as J`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ` order by id asc`
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	if v := jetton_req.Address; v != nil {
		filter_str := filterByArray("J.address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		orderby_query = ``
	}
	if v := jetton_req.OwnerAddress; v != nil {
		filter_str := filterByArray("J.owner", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
	}
	if v := jetton_req.JettonAddress; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("J.jetton = '%s'", *v))
		orderby_query = ``
	}

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query := `select ` + clmn_query
	query += ` from ` + from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	return query, nil
}

func buildJettonTransfersQuery(transfer_req JettonTransferRequest, utime_req UtimeRequest,
	lt_req LtRequest, lim_req LimitRequest, settings RequestSettings) (string, error) {
	clmn_query := `T.*`
	from_query := `jetton_transfers as T`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	if v := transfer_req.OwnerAddress; v != nil {
		if v1 := transfer_req.Direction; v1 != nil {
			f_str := ``
			if *v1 == "in" {
				f_str = filterByArray("T.destination", v)
			} else {
				f_str = filterByArray("T.source", v)
			}
			if len(f_str) > 0 {
				filter_list = append(filter_list, f_str)
			}
		} else {
			f1_str := filterByArray("T.source", v)
			f2_str := filterByArray("T.destination", v)
			if len(f1_str) > 0 {
				filter_list = append(filter_list, fmt.Sprintf("(%s or %s)", f1_str, f2_str))
			}
		}
	}
	if v := transfer_req.JettonWallet; v != nil {
		filter_str := filterByArray("T.jetton_wallet_address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
	}
	if v := transfer_req.JettonMaster; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.jetton_master_address = '%s'", *v))
	}

	order_col := "T.tx_lt"
	if v := utime_req.StartUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_now >= %d", *v))
		order_col = "T.tx_now"
	}
	if v := utime_req.EndUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_now <= %d", *v))
		order_col = "T.tx_now"
	}
	if v := lt_req.StartLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_lt >= %d", *v))
	}
	if v := lt_req.EndLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_lt <= %d", *v))
	}
	if lim_req.Sort == nil {
		lim_req.Sort = new(SortType)
		*lim_req.Sort = "desc"
	}
	if lim_req.Sort != nil {
		orderby_query = fmt.Sprintf(" order by %s %s", order_col, *lim_req.Sort)
	}

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query := `select ` + clmn_query
	query += ` from ` + from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	// log.Println(query)
	return query, nil
}

func buildActionsQuery(act_req ActionRequest, lim_req LimitRequest, settings RequestSettings) (string, error) {
	clmn_query := `A.trace_id, A.action_id, A.start_lt, A.end_lt, 
				   A.start_utime, A.end_utime, 
				   A.source, A.source_secondary, A.destination, A.destination_secondary, 
				   A.asset, A.asset_secondary, A.asset2, A.asset2_secondary, 
				   A.opcode, A.tx_hashes, A.type, A.value, A.success, 
				   (A.ton_transfer_data).content, (A.ton_transfer_data).encrypted, 
				   (A.jetton_transfer_data).response_address, (A.jetton_transfer_data).forward_amount, (A.jetton_transfer_data).query_id,
				   (A.nft_transfer_data).is_purchase, (A.nft_transfer_data).price, (A.nft_transfer_data).query_id, 
				   (A.jetton_swap_data).dex, (A.jetton_swap_data).amount_in, (A.jetton_swap_data).amount_out, (A.jetton_swap_data).peer_swaps, 
				   (A.change_dns_record_data).key, (A.change_dns_record_data).value_schema, (A.change_dns_record_data).value, (A.change_dns_record_data).flags`
	from_query := `actions as A`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	if v := act_req.ActionId; v != nil {
		filter_str := filterByArray("A.action_id", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
	}
	if v := act_req.TraceId; v != nil {
		filter_str := filterByArray("A.trace_id", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
	}

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query := `select ` + clmn_query
	query += ` from ` + from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	// log.Println(query)
	return query, nil
}

func buildEventsQuery(event_req EventRequest, utime_req UtimeRequest, lt_req LtRequest, lim_req LimitRequest, settings RequestSettings) (string, error) {
	clmn_query_default := `E.trace_id, E.external_hash, E.mc_seqno_start, E.mc_seqno_end, 
						   E.start_lt, E.start_utime, E.end_lt, E.end_utime, 
						   E.state, E.edges_, E.nodes_, E.pending_edges_, E.classification_state`
	clmn_query := clmn_query_default
	from_query := `traces as E`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	sort_order := "desc"
	if v := lim_req.Sort; v != nil {
		sort_order = string(*v)
	}

	if v := event_req.TraceId; v != nil {
		filter_str := filterByArray("E.trace_id", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
	}
	if v := event_req.AccountAddress; v != nil && len(*v) > 0 {
		filter_str := fmt.Sprintf("T.account = '%s'", *v)
		filter_list = append(filter_list, filter_str)

		from_query = `traces as E join transactions as T on E.trace_id = T.trace_id`
		clmn_query = `distinct on (E.end_lt, E.trace_id) ` + clmn_query_default
	}
	if v := event_req.TransactionHash; v != nil {
		filter_str := filterByArray("T.hash", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		from_query = `traces as E join transactions as T on E.trace_id = T.trace_id`
		clmn_query = `distinct on (E.end_lt, E.trace_id) ` + clmn_query_default
	}
	if v := event_req.MessageHash; v != nil {
		filter_str := filterByArray("M.msg_hash", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
		from_query = `traces as E join messages as M on E.trace_id = M.trace_id`
		clmn_query = `distinct on (E.end_lt, E.trace_id) ` + clmn_query_default
	}

	// time
	order_by_now := false
	if v := utime_req.StartUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("E.end_utime >= %d", *v))
		order_by_now = true
	}
	if v := utime_req.EndUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("E.end_utime <= %d", *v))
		order_by_now = true
	}
	if v := lt_req.StartLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("E.end_lt >= %d", *v))
	}
	if v := lt_req.EndLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("E.end_lt <= %d", *v))
	}

	if order_by_now {
		orderby_query = fmt.Sprintf(" order by E.end_utime %s, E.trace_id asc", sort_order)
	} else {
		orderby_query = fmt.Sprintf(" order by E.end_lt %s, E.trace_id asc", sort_order)
	}

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query := `select ` + clmn_query
	query += ` from ` + from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	return query, nil
}

func buildJettonBurnsQuery(burn_req JettonBurnRequest, utime_req UtimeRequest,
	lt_req LtRequest, lim_req LimitRequest, settings RequestSettings) (string, error) {
	clmn_query := `T.*`
	from_query := `jetton_burns as T`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	if v := burn_req.OwnerAddress; v != nil {
		f_str := ``
		f_str = filterByArray("T.owner", v)
		if len(f_str) > 0 {
			filter_list = append(filter_list, f_str)
		}
	}
	if v := burn_req.JettonWallet; v != nil {
		filter_str := filterByArray("T.jetton_wallet_address", v)
		if len(filter_str) > 0 {
			filter_list = append(filter_list, filter_str)
		}
	}
	if v := burn_req.JettonMaster; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.jetton_master_address = '%s'", *v))
	}

	order_col := "T.tx_lt"
	if v := utime_req.StartUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_now >= %d", *v))
		order_col = "T.tx_now"
	}
	if v := utime_req.EndUtime; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_now <= %d", *v))
		order_col = "T.tx_now"
	}
	if v := lt_req.StartLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_lt >= %d", *v))
	}
	if v := lt_req.EndLt; v != nil {
		filter_list = append(filter_list, fmt.Sprintf("T.tx_lt <= %d", *v))
	}
	if lim_req.Sort == nil {
		lim_req.Sort = new(SortType)
		*lim_req.Sort = "desc"
	}
	if lim_req.Sort != nil {
		orderby_query = fmt.Sprintf(" order by %s %s", order_col, *lim_req.Sort)
	}

	// build query
	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query := `select ` + clmn_query
	query += ` from ` + from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	// log.Println(query)
	return query, nil
}

func buildAccountStatesQuery(account_req AccountRequest, lim_req LimitRequest, settings RequestSettings) (string, error) {
	clmn_query_default := `A.account, A.hash, A.balance, A.account_status, A.frozen_hash, A.last_trans_hash, A.last_trans_lt, A.data_hash, A.code_hash, `
	clmn_query := clmn_query_default + `A.data_boc, A.code_boc`
	from_query := `latest_account_states as A`
	filter_list := []string{}
	filter_query := ``
	orderby_query := ``
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return "", err
	}

	// build query
	if v := account_req.AccountAddress; v != nil {
		f_str := ``
		f_str = filterByArray("A.account", v)
		if len(f_str) > 0 {
			filter_list = append(filter_list, f_str)
		}
	}
	if v := account_req.CodeHash; v != nil {
		f_str := ``
		f_str = filterByArray("A.code_hash", v)
		if len(f_str) > 0 {
			filter_list = append(filter_list, f_str)
		}
	}
	if v := account_req.IncludeBOC; v != nil && !*v {
		clmn_query = clmn_query_default + `NULL, NULL`
	}

	if len(filter_list) > 0 {
		filter_query = ` where ` + strings.Join(filter_list, " and ")
	}
	query := `select ` + clmn_query
	query += ` from ` + from_query
	query += filter_query
	query += orderby_query
	query += limit_query
	// log.Println(query)
	return query, nil
}

// query implementation functions
func queryBlocksImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]Block, error) {
	// blocks
	blks := []Block{}
	{
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("query timeout %v", settings.Timeout)
		default:
		}
		defer rows.Close()

		for rows.Next() {
			if blk, err := ScanBlock(rows); err == nil {
				blks = append(blks, *blk)
			} else {
				return nil, IndexError{Code: 500, Message: err.Error()}
			}
		}
	}
	return blks, nil
}

func queryMessagesImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]Message, error) {
	msgs := []Message{}
	{
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		// select {
		// case <-ctx.Done():
		// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
		// default:
		// }
		defer rows.Close()

		for rows.Next() {
			msg, err := ScanMessageWithContent(rows)
			if err != nil {
				return nil, IndexError{Code: 500, Message: err.Error()}
			}
			msgs = append(msgs, *msg)
		}
		if rows.Err() != nil {
			return nil, IndexError{Code: 500, Message: rows.Err().Error()}
		}
	}
	return msgs, nil
}

func queryBlockExists(seqno int32, conn *pgxpool.Conn, settings RequestSettings) (bool, error) {
	query := fmt.Sprintf(`select seqno from blocks where workchain = -1 and shard = -9223372036854775808 and seqno = %d`, seqno)
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return false, IndexError{Code: 500, Message: err.Error()}
	}

	seqnos := []int32{}
	for rows.Next() {
		var s int32
		if err := rows.Scan(&s); err != nil {
			return false, err
		}
		seqnos = append(seqnos, s)
	}
	if rows.Err() != nil {
		return false, IndexError{Code: 500, Message: rows.Err().Error()}
	}
	return len(seqnos) > 0, nil
}

func queryTransactionsImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]Transaction, error) {
	// transactions
	txs := []Transaction{}
	txs_map := map[HashType]int{}
	{
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		// select {
		// case <-ctx.Done():
		// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
		// default:
		// }
		defer rows.Close()

		for rows.Next() {
			if tx, err := ScanTransaction(rows); err == nil {
				txs = append(txs, *tx)
				txs_map[tx.Hash] = len(txs) - 1
			} else {
				return nil, IndexError{Code: 500, Message: err.Error()}
			}
		}
		if rows.Err() != nil {
			return nil, IndexError{Code: 500, Message: rows.Err().Error()}
		}
	}

	acst_list := []string{}
	hash_list := []string{}
	for _, t := range txs {
		hash_list = append(hash_list, fmt.Sprintf("'%s'", t.Hash))
		acst_list = append(acst_list, fmt.Sprintf("'%s'", t.AccountStateHashBefore))
		acst_list = append(acst_list, fmt.Sprintf("'%s'", t.AccountStateHashAfter))
	}
	// account states
	if len(txs) == 0 {
		return txs, nil
	}
	if len(acst_list) > 0 {
		acst_list_str := strings.Join(acst_list, ",")
		query = fmt.Sprintf("select * from account_states where hash in (%s)", acst_list_str)

		acsts, err := queryAccountStatesImpl(query, conn, settings)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		acsts_map := make(map[HashType]*AccountState)
		for _, a := range acsts {
			acsts_map[a.Hash] = &a
		}
		for idx := range txs {
			if v, ok := acsts_map[txs[idx].AccountStateHashBefore]; ok {
				txs[idx].AccountStateBefore = v
			}
			if v, ok := acsts_map[txs[idx].AccountStateHashAfter]; ok {
				txs[idx].AccountStateAfter = v
			}
		}
	}

	// messages
	if len(hash_list) > 0 {
		hash_list_str := strings.Join(hash_list, ",")
		query = fmt.Sprintf(`select M.*, B.*, I.* from messages as M 
			left join message_contents as B on M.body_hash = B.hash 
			left join message_contents as I on M.init_state_hash = I.hash
			where M.tx_hash in (%s)`, hash_list_str)

		msgs, err := queryMessagesImpl(query, conn, settings)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}

		for _, msg := range msgs {
			if msg.Direction == "in" {
				txs[txs_map[msg.TxHash]].InMsg = &msg
			} else {
				txs[txs_map[msg.TxHash]].OutMsgs = append(txs[txs_map[msg.TxHash]].OutMsgs, &msg)
			}
		}
	}

	// sort messages
	for idx := range txs {
		sort.SliceStable(txs[idx].OutMsgs, func(i, j int) bool {
			if txs[idx].OutMsgs[i].CreatedLt == nil {
				return true
			}
			if txs[idx].OutMsgs[j].CreatedLt == nil {
				return false
			}
			return *txs[idx].OutMsgs[i].CreatedLt < *txs[idx].OutMsgs[j].CreatedLt
		})
	}
	return txs, nil
}

func queryAdjacentTransactionsImpl(req AdjacentTransactionRequest, conn *pgxpool.Conn, settings RequestSettings) ([]string, error) {
	// transactions
	txs := []string{}
	query := fmt.Sprintf(`select M2.tx_hash from messages as M1 join messages as M2 on M1.msg_hash = M2.msg_hash and M1.direction != M2.direction where M1.tx_hash = '%s'`, req.Hash)
	if req.Direction != nil && (*req.Direction == "in" || *req.Direction == "out") {
		query += fmt.Sprintf(" and M1.direction = '%s'", *req.Direction)
	}
	{
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		// select {
		// case <-ctx.Done():
		// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
		// default:
		// }
		defer rows.Close()

		for rows.Next() {
			var tx string
			err := rows.Scan(&tx)
			if err != nil {
				return nil, err
			}
			txs = append(txs, tx)
		}
		if rows.Err() != nil {
			return nil, IndexError{Code: 500, Message: rows.Err().Error()}
		}
	}
	return txs, nil
}

func queryAddressBookImpl(addr_list []string, conn *pgxpool.Conn, settings RequestSettings) (AddressBook, error) {
	book := AddressBook{}
	{
		addr_list_str := strings.Join(addr_list, ",")
		query := fmt.Sprintf("select account, account_friendly, code_hash, account_status from latest_account_states where account in (%s)", addr_list_str)

		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		// select {
		// case <-ctx.Done():
		// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
		// default:
		// }
		defer rows.Close()

		for rows.Next() {
			var account string
			var account_friendly *string
			var code_hash *string
			var account_status *string
			if err := rows.Scan(&account, &account_friendly, &code_hash, &account_status); err == nil {
				addr_str := getAccountAddressFriendly(account, code_hash, account_status, settings.IsTestnet)
				book[strings.Trim(account, " ")] = AddressBookRow{UserFriendly: &addr_str}
			} else {
				return nil, IndexError{Code: 500, Message: err.Error()}
			}
		}
		if rows.Err() != nil {
			return nil, IndexError{Code: 500, Message: rows.Err().Error()}
		}
	}
	return book, nil
}

func queryAccountStatesImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]AccountState, error) {
	acsts := []AccountState{}
	{
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		// select {
		// case <-ctx.Done():
		// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
		// default:
		// }
		defer rows.Close()

		for rows.Next() {
			if acst, err := ScanAccountState(rows); err == nil {
				acsts = append(acsts, *acst)
			} else {
				return nil, IndexError{Code: 500, Message: err.Error()}
			}
		}
		if rows.Err() != nil {
			return nil, IndexError{Code: 500, Message: rows.Err().Error()}
		}
	}
	return acsts, nil
}

func queryAccountStateFullImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]AccountStateFull, error) {
	acsts := []AccountStateFull{}
	{
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		// select {
		// case <-ctx.Done():
		// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
		// default:
		// }
		defer rows.Close()

		for rows.Next() {
			if acst, err := ScanAccountStateFull(rows); err == nil {
				acsts = append(acsts, *acst)
			} else {
				return nil, IndexError{Code: 500, Message: err.Error()}
			}
		}
		if rows.Err() != nil {
			return nil, IndexError{Code: 500, Message: rows.Err().Error()}
		}
	}
	return acsts, nil
}

func queryTopAccountBalancesImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]AccountBalance, error) {
	acsts := []AccountBalance{}
	{
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		defer rows.Close()

		for rows.Next() {
			if acst, err := ScanAccountBalance(rows); err == nil {
				acsts = append(acsts, *acst)
			} else {
				return nil, IndexError{Code: 500, Message: err.Error()}
			}
		}
		if rows.Err() != nil {
			return nil, IndexError{Code: 500, Message: rows.Err().Error()}
		}
	}
	return acsts, nil
}

func queryNFTCollectionsImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]NFTCollection, error) {
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	// select {
	// case <-ctx.Done():
	// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
	// default:
	// }
	defer rows.Close()

	nfts := []NFTCollection{}
	for rows.Next() {
		if nft, err := ScanNFTCollection(rows); err == nil {
			nfts = append(nfts, *nft)
		} else {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	if rows.Err() != nil {
		return nil, IndexError{Code: 500, Message: rows.Err().Error()}
	}

	return nfts, nil
}

func queryNFTItemsWithCollectionsImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]NFTItem, error) {
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	// select {
	// case <-ctx.Done():
	// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
	// default:
	// }
	defer rows.Close()

	nfts := []NFTItem{}
	for rows.Next() {
		if nft, err := ScanNFTItemWithCollection(rows); err == nil {
			nfts = append(nfts, *nft)
		} else {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	if rows.Err() != nil {
		return nil, IndexError{Code: 500, Message: rows.Err().Error()}
	}
	return nfts, nil
}

func queryNFTTransfersImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]NFTTransfer, error) {
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	// select {
	// case <-ctx.Done():
	// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
	// default:
	// }
	defer rows.Close()

	res := []NFTTransfer{}
	for rows.Next() {
		if loc, err := ScanNFTTransfer(rows); err == nil {
			res = append(res, *loc)
		} else {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	if rows.Err() != nil {
		return nil, IndexError{Code: 500, Message: rows.Err().Error()}
	}
	return res, nil
}

func queryJettonMastersImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]JettonMaster, error) {
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	// select {
	// case <-ctx.Done():
	// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
	// default:
	// }
	defer rows.Close()

	res := []JettonMaster{}
	for rows.Next() {
		if loc, err := ScanJettonMaster(rows); err == nil {
			res = append(res, *loc)
		} else {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	if rows.Err() != nil {
		return nil, IndexError{Code: 500, Message: rows.Err().Error()}
	}
	return res, nil
}

func queryJettonWalletsImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]JettonWallet, error) {
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	// select {
	// case <-ctx.Done():
	// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
	// default:
	// }
	defer rows.Close()

	res := []JettonWallet{}
	for rows.Next() {
		if loc, err := ScanJettonWallet(rows); err == nil {
			res = append(res, *loc)
		} else {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	if rows.Err() != nil {
		return nil, IndexError{Code: 500, Message: rows.Err().Error()}
	}
	return res, nil
}

func queryJettonTransfersImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]JettonTransfer, error) {
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	// select {
	// case <-ctx.Done():
	// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
	// default:
	// }
	defer rows.Close()

	res := []JettonTransfer{}
	for rows.Next() {
		if loc, err := ScanJettonTransfer(rows); err == nil {
			res = append(res, *loc)
		} else {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	if rows.Err() != nil {
		return nil, IndexError{Code: 500, Message: rows.Err().Error()}
	}
	return res, nil
}

func queryJettonBurnsImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]JettonBurn, error) {
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	// select {
	// case <-ctx.Done():
	// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
	// default:
	// }
	defer rows.Close()

	res := []JettonBurn{}
	for rows.Next() {
		if loc, err := ScanJettonBurn(rows); err == nil {
			res = append(res, *loc)
		} else {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	if rows.Err() != nil {
		return nil, IndexError{Code: 500, Message: rows.Err().Error()}
	}
	return res, nil
}

func queryRawActionsImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]RawAction, error) {
	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	// select {
	// case <-ctx.Done():
	// 	return nil, fmt.Errorf("query timeout %v", settings.Timeout)
	// default:
	// }
	defer rows.Close()

	res := []RawAction{}
	for rows.Next() {
		if loc, err := ScanRawAction(rows); err == nil {
			res = append(res, *loc)
		} else {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	if rows.Err() != nil {
		return nil, IndexError{Code: 500, Message: rows.Err().Error()}
	}
	return res, nil
}

func queryEventsImpl(query string, conn *pgxpool.Conn, settings RequestSettings) ([]Event, error) {
	events := []Event{}
	{
		ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
		defer cancel_ctx()
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return nil, IndexError{Code: 500, Message: err.Error()}
		}
		defer rows.Close()

		for rows.Next() {
			if loc, err := ScanEvent(rows); err == nil {
				loc.Transactions = make(map[HashType]*Transaction)
				events = append(events, *loc)
			} else {
				return nil, IndexError{Code: 500, Message: err.Error()}
			}
		}
		if rows.Err() != nil {
			return nil, IndexError{Code: 500, Message: rows.Err().Error()}
		}
	}
	events_map := map[HashType]int{}
	trace_id_list := []HashType{}
	for idx, event := range events {
		events_map[event.TraceId] = idx
		trace_id_list = append(trace_id_list, event.TraceId)
	}
	if len(trace_id_list) > 0 {
		{
			query := `select A.trace_id, A.action_id, A.start_lt, A.end_lt, 
				  A.start_utime, A.end_utime, 
				  A.source, A.source_secondary, A.destination, A.destination_secondary, 
				  A.asset, A.asset_secondary, A.asset2, A.asset2_secondary, 
				  A.opcode, A.tx_hashes, A.type, A.value, A.success, 
				  (A.ton_transfer_data).content, (A.ton_transfer_data).encrypted, 
				  (A.jetton_transfer_data).response_address, (A.jetton_transfer_data).forward_amount, (A.jetton_transfer_data).query_id,
				  (A.nft_transfer_data).is_purchase, (A.nft_transfer_data).price, (A.nft_transfer_data).query_id, 
				  (A.jetton_swap_data).dex, (A.jetton_swap_data).amount_in, (A.jetton_swap_data).amount_out, (A.jetton_swap_data).peer_swaps, 
				  (A.change_dns_record_data).key, (A.change_dns_record_data).value_schema, (A.change_dns_record_data).value, (A.change_dns_record_data).flags
				  from actions as A where ` + filterByArray("A.trace_id", trace_id_list) + ` order by trace_id, start_lt, end_lt`
			actions, err := queryRawActionsImpl(query, conn, settings)
			if err != nil {
				return nil, IndexError{Code: 500, Message: fmt.Sprintf("failed query actions: %s", err.Error())}
			}
			for idx := range actions {
				raw_action := &actions[idx]
				action, err := ParseRawAction(raw_action)
				if err != nil {
					return nil, IndexError{Code: 500, Message: fmt.Sprintf("failed to parse action: %s", err.Error())}
				}
				events[events_map[action.TraceId]].Actions = append(events[events_map[action.TraceId]].Actions, action)
			}
		}
		{
			query := `select T.* from transactions as T where ` + filterByArray("T.trace_id", trace_id_list) + ` order by trace_id, lt`
			txs, err := queryTransactionsImpl(query, conn, settings)
			if err != nil {
				return nil, IndexError{Code: 500, Message: fmt.Sprintf("failed query transactions: %s", err.Error())}
			}
			for idx := range txs {
				tx := &txs[idx]
				if v := tx.TraceId; v != nil {
					event := &events[events_map[*v]]
					event.TransactionsOrder = append(event.TransactionsOrder, tx.Hash)
					event.Transactions[tx.Hash] = tx
				}
			}
		}
	}
	for idx := range events {
		if len(events[idx].TransactionsOrder) > 0 {
			trace, err := assembleEventTraceFromMap(&events[idx].TransactionsOrder, &events[idx].Transactions)
			if err != nil {
				return nil, IndexError{Code: 500, Message: fmt.Sprintf("failed to assemble trace: %s", err.Error())}
			}
			events[idx].Trace = trace
		}
	}
	return events, nil
}

func assembleEventTraceFromMap(tx_order *[]HashType, txs *map[HashType]*Transaction) (*TraceNode, error) {
	nodes := map[HashType]*TraceNode{}
	var root *TraceNode = nil
	for _, tx_hash := range *tx_order {
		tx := (*txs)[tx_hash]
		var in_msg_hash HashType
		if in_msg := tx.InMsg; in_msg != nil {
			in_msg_hash = in_msg.MsgHash
		}
		node := TraceNode{TransactionHash: tx_hash, InMsgHash: in_msg_hash}
		if len(tx.OutMsgs) == 0 {
			node.Children = make([]*TraceNode, 0)
		}
		for _, msg := range tx.OutMsgs {
			nodes[msg.MsgHash] = &node
		}
		if parent, ok := nodes[in_msg_hash]; ok {
			delete(nodes, in_msg_hash)
			parent.Children = append(parent.Children, &node)
		} else if root == nil {
			root = &node
		} else {
			return nil, fmt.Errorf("failed to build trace: unreachable branch of code")
		}
	}
	return root, nil
}

// Exported methods
func (db *DbClient) QueryMasterchainInfo(settings RequestSettings) (*MasterchainInfo, error) {
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	ctx, cancel_ctx := context.WithTimeout(context.Background(), settings.Timeout)
	defer cancel_ctx()
	res := conn.QueryRow(ctx, "select * from blocks where workchain = -1 order by seqno desc limit 1")
	last, err := ScanBlock(res)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}

	res = conn.QueryRow(ctx, "select * from blocks where workchain = -1 order by seqno asc limit 1")
	first, err := ScanBlock(res)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}

	info := MasterchainInfo{last, first}
	return &info, nil
}

func (db *DbClient) QueryBlocks(
	blk_req BlockRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]Block, error) {
	query, err := buildBlocksQuery(blk_req, utime_req, lt_req, lim_req, settings)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()
	return queryBlocksImpl(query, conn, settings)
}

func (db *DbClient) QueryShards(
	seqno int,
	settings RequestSettings,
) ([]Block, error) {
	query := fmt.Sprintf(`select B.* from shard_state as S join blocks as B 
		on S.workchain = B.workchain and S.shard = B.shard and S.seqno = B.seqno 
		where mc_seqno = %d 
		order by S.mc_seqno, S.workchain, S.shard, S.seqno`, seqno)
	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()
	return queryBlocksImpl(query, conn, settings)
}

func (db *DbClient) QueryAddressBook(
	addr_list []string,
	settings RequestSettings,
) (AddressBook, error) {
	raw_addr_list := []string{}
	raw_addr_map := map[string]string{}
	for _, addr := range addr_list {
		addr_loc := AccountAddressConverter(addr)
		if addr_loc.IsValid() {
			if v, ok := addr_loc.Interface().(string); ok {
				raw_addr_list = append(raw_addr_list, fmt.Sprintf("'%s'", v))
				raw_addr_map[addr] = v
			}
		} else {
			raw_addr_map[addr] = ""
		}
	}
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()
	book, err := queryAddressBookImpl(raw_addr_list, conn, settings)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}

	new_addr_book := AddressBook{}
	for k, v := range raw_addr_map {
		if vv, ok := book[v]; ok {
			new_addr_book[k] = vv
		} else {
			new_addr_book[k] = AddressBookRow{nil}
		}

	}
	return new_addr_book, nil
}

func (db *DbClient) QueryTransactions(
	blk_req BlockRequest,
	tx_req TransactionRequest,
	msg_req MessageRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]Transaction, AddressBook, error) {
	query, err := buildTransactionsQuery(blk_req, tx_req, msg_req, utime_req, lt_req, lim_req, settings)
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	// check block
	if seqno := blk_req.McSeqno; seqno != nil {
		exists, err := queryBlockExists(*seqno, conn, settings)
		if err != nil {
			return nil, nil, err
		}
		if !exists {
			return nil, nil, IndexError{Code: 404, Message: fmt.Sprintf("masterchain block %d not found", *seqno)}
		}
	}

	txs, err := queryTransactionsImpl(query, conn, settings)
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	book := AddressBook{}
	addr_list := []string{}
	for _, t := range txs {
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.Account))
		if t.InMsg != nil {
			if t.InMsg.Source != nil {
				addr_list = append(addr_list, fmt.Sprintf("'%s'", *t.InMsg.Source))
			}
		}
		for _, m := range t.OutMsgs {
			if m.Destination != nil {
				addr_list = append(addr_list, fmt.Sprintf("'%s'", *m.Destination))
			}
		}
	}
	if len(addr_list) > 0 {
		book, err = queryAddressBookImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	return txs, book, nil
}

func (db *DbClient) QueryAdjacentTransactions(
	req AdjacentTransactionRequest,
	settings RequestSettings,
) ([]Transaction, AddressBook, error) {
	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	tx_hash_list, err := queryAdjacentTransactionsImpl(req, conn, settings)
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	if len(tx_hash_list) == 0 {
		return nil, nil, IndexError{Code: 404, Message: "adjacent transactions not found"}
	}

	for idx := range tx_hash_list {
		tx_hash_list[idx] = fmt.Sprintf("'%s'", tx_hash_list[idx])
	}
	tx_hash_str := strings.Join(tx_hash_list, ",")
	query := fmt.Sprintf("select * from transactions where hash in (%s) order by lt asc", tx_hash_str)
	txs, err := queryTransactionsImpl(query, conn, settings)
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	book := AddressBook{}
	addr_list := []string{}
	for _, t := range txs {
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.Account))
		if t.InMsg != nil {
			if t.InMsg.Source != nil {
				addr_list = append(addr_list, fmt.Sprintf("'%s'", *t.InMsg.Source))
			}
		}
		for _, m := range t.OutMsgs {
			if m.Destination != nil {
				addr_list = append(addr_list, fmt.Sprintf("'%s'", *m.Destination))
			}
		}
	}
	if len(addr_list) > 0 {
		book, err = queryAddressBookImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	return txs, book, nil
}

func (db *DbClient) QueryMessages(
	msg_req MessageRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]Message, AddressBook, error) {
	query, err := buildMessagesQuery(msg_req, utime_req, lt_req, lim_req, settings)
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	msgs, err := queryMessagesImpl(query, conn, settings)
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	book := AddressBook{}
	addr_list := []string{}
	for _, m := range msgs {
		if m.Source != nil {
			addr_list = append(addr_list, fmt.Sprintf("'%s'", *m.Source))
		}
		if m.Destination != nil {
			addr_list = append(addr_list, fmt.Sprintf("'%s'", *m.Destination))
		}
	}
	if len(addr_list) > 0 {
		book, err = queryAddressBookImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	return msgs, book, nil
}

func (db *DbClient) QueryNFTCollections(
	nft_req NFTCollectionRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]NFTCollection, AddressBook, error) {
	query, err := buildNFTCollectionsQuery(nft_req, lim_req, settings)
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	res, err := queryNFTCollectionsImpl(query, conn, settings)
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	book := AddressBook{}
	addr_list := []string{}
	for _, t := range res {
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.Address))
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.OwnerAddress))
	}
	if len(addr_list) > 0 {
		book, err = queryAddressBookImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	return res, book, nil
}

func (db *DbClient) QueryNFTItems(
	nft_req NFTItemRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]NFTItem, AddressBook, error) {
	query, err := buildNFTItemsQuery(nft_req, lim_req, settings)
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	res, err := queryNFTItemsWithCollectionsImpl(query, conn, settings)
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	book := AddressBook{}
	addr_list := []string{}
	for _, t := range res {
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.Address))
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.OwnerAddress))
	}
	if len(addr_list) > 0 {
		book, err = queryAddressBookImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	return res, book, nil
}

func (db *DbClient) QueryNFTTransfers(
	transfer_req NFTTransferRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]NFTTransfer, AddressBook, error) {
	query, err := buildNFTTransfersQuery(transfer_req, utime_req, lt_req, lim_req, settings)
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	res, err := queryNFTTransfersImpl(query, conn, settings)
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	book := AddressBook{}
	addr_list := []string{}
	for _, t := range res {
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.NftItemAddress))
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.NftCollectionAddress))
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.OldOwner))
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.NewOwner))
		if t.ResponseDestination != nil {
			addr_list = append(addr_list, fmt.Sprintf("'%s'", *t.ResponseDestination))
		}
	}
	if len(addr_list) > 0 {
		book, err = queryAddressBookImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	return res, book, nil
}

func (db *DbClient) QueryJettonMasters(
	jetton_req JettonMasterRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]JettonMaster, AddressBook, error) {
	query, err := buildJettonMastersQuery(jetton_req, lim_req, settings)
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	res, err := queryJettonMastersImpl(query, conn, settings)
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	book := AddressBook{}
	addr_list := []string{}
	for _, t := range res {
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.Address))
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.AdminAddress))
	}
	if len(addr_list) > 0 {
		book, err = queryAddressBookImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	return res, book, nil
}

func (db *DbClient) QueryJettonWallets(
	jetton_req JettonWalletRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]JettonWallet, AddressBook, error) {
	query, err := buildJettonWalletsQuery(jetton_req, lim_req, settings)
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	res, err := queryJettonWalletsImpl(query, conn, settings)
	if err != nil {
		log.Println(query)
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	book := AddressBook{}
	addr_list := []string{}
	for _, t := range res {
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.Address))
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.Owner))
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.Jetton))
	}
	if len(addr_list) > 0 {
		book, err = queryAddressBookImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	return res, book, nil
}

func (db *DbClient) QueryJettonTransfers(
	transfer_req JettonTransferRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]JettonTransfer, AddressBook, error) {
	query, err := buildJettonTransfersQuery(transfer_req, utime_req, lt_req, lim_req, settings)
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	res, err := queryJettonTransfersImpl(query, conn, settings)
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	book := AddressBook{}
	addr_list := []string{}
	for _, t := range res {
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.Source))
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.Destination))
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.SourceWallet))
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.JettonMaster))
		if t.ResponseDestination != nil {
			addr_list = append(addr_list, fmt.Sprintf("'%s'", *t.ResponseDestination))
		}
	}
	if len(addr_list) > 0 {
		book, err = queryAddressBookImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	return res, book, nil
}

func (db *DbClient) QueryJettonBurns(
	transfer_req JettonBurnRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]JettonBurn, AddressBook, error) {
	query, err := buildJettonBurnsQuery(transfer_req, utime_req, lt_req, lim_req, settings)
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	res, err := queryJettonBurnsImpl(query, conn, settings)
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	book := AddressBook{}
	addr_list := []string{}
	for _, t := range res {
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.Owner))
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.JettonWallet))
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.JettonMaster))
		if t.ResponseDestination != nil {
			addr_list = append(addr_list, fmt.Sprintf("'%s'", *t.ResponseDestination))
		}
	}
	if len(addr_list) > 0 {
		book, err = queryAddressBookImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	return res, book, nil
}

func (db *DbClient) QueryAccountStates(
	account_req AccountRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]AccountStateFull, AddressBook, error) {
	query, err := buildAccountStatesQuery(account_req, lim_req, settings)
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	res, err := queryAccountStateFullImpl(query, conn, settings)
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	book := AddressBook{}
	addr_list := []string{}
	for _, t := range res {
		addr_list = append(addr_list, fmt.Sprintf("'%s'", t.AccountAddress))
	}
	if len(addr_list) > 0 {
		book, err = queryAddressBookImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	return res, book, nil
}

func (db *DbClient) QueryWalletStates(
	account_req AccountRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]WalletState, AddressBook, error) {
	states, book, err := db.QueryAccountStates(account_req, lim_req, settings)
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	res := []WalletState{}
	for _, state := range states {
		loc, err := ParseWalletState(state)
		if err != nil {
			return nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
		res = append(res, *loc)
	}
	return res, book, nil
}

func (db *DbClient) QueryTopAccountBalances(lim_req LimitRequest, settings RequestSettings) ([]AccountBalance, error) {
	limit_query, err := limitQuery(lim_req, settings)
	if err != nil {
		return nil, err
	}
	query := `select account, balance from latest_account_states order by balance desc` + limit_query

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	res, err := queryTopAccountBalancesImpl(query, conn, settings)
	if err != nil {
		return nil, IndexError{Code: 500, Message: err.Error()}
	}
	return res, nil
}

// events
func (db *DbClient) QueryActions(
	act_req ActionRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]Action, AddressBook, error) {
	query, err := buildActionsQuery(act_req, lim_req, settings)
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	raw_actions, err := queryRawActionsImpl(query, conn, settings)
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	book := AddressBook{}
	addr_list := []string{}
	for _, t := range raw_actions {
		if v := t.Source; v != nil {
			addr_list = append(addr_list, fmt.Sprintf("'%s'", *v))
		}
		if v := t.SourceSecondary; v != nil {
			addr_list = append(addr_list, fmt.Sprintf("'%s'", *v))
		}
		if v := t.Destination; v != nil {
			addr_list = append(addr_list, fmt.Sprintf("'%s'", *v))
		}
		if v := t.DestinationSecondary; v != nil {
			addr_list = append(addr_list, fmt.Sprintf("'%s'", *v))
		}
		if v := t.Asset; v != nil {
			addr_list = append(addr_list, fmt.Sprintf("'%s'", *v))
		}
		if v := t.AssetSecondary; v != nil {
			addr_list = append(addr_list, fmt.Sprintf("'%s'", *v))
		}
		if v := t.Asset2; v != nil {
			addr_list = append(addr_list, fmt.Sprintf("'%s'", *v))
		}
		if v := t.Asset2Secondary; v != nil {
			addr_list = append(addr_list, fmt.Sprintf("'%s'", *v))
		}
		if v := t.JettonTransferResponseAddress; v != nil {
			addr_list = append(addr_list, fmt.Sprintf("'%s'", *v))
		}
	}
	if len(addr_list) > 0 {
		book, err = queryAddressBookImpl(addr_list, conn, settings)
		if err != nil {
			return nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
	}
	actions := []Action{}
	for idx := range raw_actions {
		action, err := ParseRawAction(&raw_actions[idx])
		if err != nil {
			return nil, nil, IndexError{Code: 500, Message: err.Error()}
		}
		actions = append(actions, *action)
	}
	return actions, book, nil
}

func (db *DbClient) QueryEvents(
	event_req EventRequest,
	utime_req UtimeRequest,
	lt_req LtRequest,
	lim_req LimitRequest,
	settings RequestSettings,
) ([]Event, AddressBook, error) {
	query, err := buildEventsQuery(event_req, utime_req, lt_req, lim_req, settings)
	// log.Println(query)
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	// read data
	conn, err := db.Pool.Acquire(context.Background())
	if err != nil {
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}
	defer conn.Release()

	res, err := queryEventsImpl(query, conn, settings)
	if err != nil {
		log.Println(query)
		return nil, nil, IndexError{Code: 500, Message: err.Error()}
	}

	book := AddressBook{}
	return res, book, nil
}
