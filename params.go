package pgxexec

import "strconv"

type params struct {
	index uint64
	args  []interface{}
}

func (p *params) get(val interface{}) string {
	p.index++
	p.args = append(p.args, val)

	return "$" + strconv.FormatUint(p.index, 10)
}

func (p *params) getStartsWith(val interface{}) string {
	p.index++
	p.args = append(p.args, val.(string)+"%")

	return "$" + strconv.FormatUint(p.index, 10)
}

func (p *params) getEndsWith(val interface{}) string {
	p.index++
	p.args = append(p.args, "%"+val.(string))

	return "$" + strconv.FormatUint(p.index, 10)
}

func (p *params) getContains(val interface{}) string {
	p.index++
	p.args = append(p.args, "%"+val.(string)+"%")

	return "$" + strconv.FormatUint(p.index, 10)
}
