package index

func BinarySearchBy(size int, f func(int) int) (int, bool) {
	left := 0
	right := size
	for left < right {
		mid := left + size/2
		cmp := f(mid)
		if cmp < 0 {
			left = mid + 1
		} else if cmp > 0 {
			right = mid
		} else {
			// 探索成功
			return mid, true
		}
		size = right - left
	}
	return left, false
}
