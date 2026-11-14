package store

import (
	"math"
	"math/big"
	"strconv"
)

// SLS 输出 double 时用 Grisu2 算法生成数字，而且不做末位修正（Loitsch 论文里的原始版本，RapidJSON 在此基础上
// 加了 GrisuRound）：得到的数字串能还原成同一个 double，但不一定是离真值最近的那个（0.1+0.2 输出
// 0.30000000000000007，而不是 0.30000000000000004）。实测与真实 SLS 的输出逐位一致（dtoa_test.go）。
// 代码结构参照 RapidJSON 的 internal/dtoa.h、internal/diyfp.h（MIT 许可，见 NOTICE），排版规则（Prettify）相同。

type diyFp struct {
	f uint64
	e int
}

const (
	dpSignificandSize = 52
	dpExponentBias    = 0x3FF + dpSignificandSize
	dpMinExponent     = -dpExponentBias
	dpHiddenBit       = uint64(1) << dpSignificandSize
	dpSignificandMask = dpHiddenBit - 1
)

func newDiyFp(d float64) diyFp {
	u := math.Float64bits(d)
	be := int((u >> dpSignificandSize) & 0x7FF)
	sig := u & dpSignificandMask
	if be != 0 {
		return diyFp{sig + dpHiddenBit, be - dpExponentBias}
	}
	return diyFp{sig, dpMinExponent + 1}
}

func (x diyFp) mul(y diyFp) diyFp {
	const m32 = 0xFFFFFFFF
	a, b := x.f>>32, x.f&m32
	c, d := y.f>>32, y.f&m32
	ac, bc, ad, bd := a*c, b*c, a*d, b*d
	tmp := (bd >> 32) + (ad & m32) + (bc & m32)
	tmp += 1 << 31 // 四舍五入
	return diyFp{ac + (ad >> 32) + (bc >> 32) + (tmp >> 32), x.e + y.e + 64}
}

func (x diyFp) normalizeBoundary() diyFp {
	for x.f&(dpHiddenBit<<1) == 0 {
		x.f <<= 1
		x.e--
	}
	x.f <<= 64 - dpSignificandSize - 2
	x.e -= 64 - dpSignificandSize - 2
	return x
}

func (x diyFp) normalizedBoundaries() (minus, plus diyFp) {
	pl := diyFp{(x.f << 1) + 1, x.e - 1}.normalizeBoundary()
	var mi diyFp
	if x.f == dpHiddenBit {
		mi = diyFp{(x.f << 2) - 1, x.e - 2}
	} else {
		mi = diyFp{(x.f << 1) - 1, x.e - 1}
	}
	mi.f <<= uint(mi.e - pl.e)
	mi.e = pl.e
	return mi, pl
}

// cachedPowers[i] ≈ 10^(-348+8i)，64 位规格化有效数字，四舍五入（与 RapidJSON 的表相同）。
var cachedPowers = func() [87]diyFp {
	var out [87]diyFp
	for i := range out {
		k := -348 + 8*i
		v := new(big.Float).SetPrec(512).SetInt64(1)
		ten := new(big.Float).SetPrec(512).SetInt64(10)
		for j := 0; j < abs(k); j++ {
			if k > 0 {
				v.Mul(v, ten)
			} else {
				v.Quo(v, ten)
			}
		}
		e := v.MantExp(nil) - 64 // v = m × 2^e，m ∈ [2^63, 2^64)
		m := new(big.Float).SetPrec(512).SetMantExp(v, -e)
		m.Add(m, big.NewFloat(0.5))
		mi, _ := m.Int(nil)
		if mi.BitLen() > 64 {
			mi.Rsh(mi, 1)
			e++
		}
		out[i] = diyFp{mi.Uint64(), e}
	}
	return out
}()

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

func cachedPower(e int) (diyFp, int) {
	dk := float64(-61-e)*0.30102999566398114 + 347
	k := int(dk)
	if dk-float64(k) > 0 {
		k++
	}
	index := (k >> 3) + 1
	return cachedPowers[index], -(-348 + index*8)
}

var pow10 = [...]uint64{1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000,
	10000000000, 100000000000, 1000000000000, 10000000000000, 100000000000000, 1000000000000000,
	10000000000000000, 100000000000000000, 1000000000000000000, 10000000000000000000}

func countDecimalDigit32(n uint32) int {
	d := 1
	for n >= 10 {
		n /= 10
		d++
	}
	return d
}

func digitGen(mp diyFp, delta uint64, k int) ([]byte, int) {
	one := diyFp{uint64(1) << uint(-mp.e), mp.e}
	p1 := uint32(mp.f >> uint(-one.e))
	p2 := mp.f & (one.f - 1)
	kappa := countDecimalDigit32(p1)
	var buf []byte
	for kappa > 0 {
		div := uint32(pow10[kappa-1])
		d := p1 / div
		p1 %= div
		if d != 0 || len(buf) > 0 {
			buf = append(buf, byte('0'+d))
		}
		kappa--
		tmp := (uint64(p1) << uint(-one.e)) + p2
		if tmp <= delta {
			return buf, k + kappa
		}
	}
	for {
		p2 *= 10
		delta *= 10
		d := byte(p2 >> uint(-one.e))
		if d != 0 || len(buf) > 0 {
			buf = append(buf, '0'+d)
		}
		p2 &= one.f - 1
		kappa--
		if p2 < delta {
			return buf, k + kappa
		}
	}
}

func grisu2(v float64) ([]byte, int) {
	wm, wp := newDiyFp(v).normalizedBoundaries()
	cmk, k := cachedPower(wp.e)
	wP := wp.mul(cmk)
	wM := wm.mul(cmk)
	wM.f++
	wP.f--
	return digitGen(wP, wP.f-wM.f, k)
}

// prettify 按 RapidJSON 的规则排版：整数补 ".0"，21 位以内不用科学计数法。
func prettify(buf []byte, k int) string {
	n := len(buf)
	kk := n + k // 10^(kk-1) <= v < 10^kk
	switch {
	case k >= 0 && kk <= 21:
		out := append([]byte{}, buf...)
		for i := n; i < kk; i++ {
			out = append(out, '0')
		}
		return string(out) + ".0"
	case kk > 0 && kk <= 21:
		return string(buf[:kk]) + "." + string(buf[kk:])
	case kk > -6 && kk <= 0:
		out := []byte("0.")
		for i := 0; i < -kk; i++ {
			out = append(out, '0')
		}
		return string(append(out, buf...))
	case kk < -324:
		return "0.0"
	case n == 1:
		return string(buf) + "e" + strconv.Itoa(kk-1)
	default:
		return string(buf[:1]) + "." + string(buf[1:]) + "e" + strconv.Itoa(kk-1)
	}
}

// formatGrisu 是 SLS 的 double 文本格式。
func formatGrisu(f float64) string {
	if f == 0 {
		if math.Signbit(f) {
			return "-0.0"
		}
		return "0.0"
	}
	sign := ""
	if f < 0 {
		sign, f = "-", -f
	}
	buf, k := grisu2(f)
	return sign + prettify(buf, k)
}
