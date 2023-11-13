package redis

import "unsafe"

func uint8Slice2String(input []uint8) (output string) {
	output = string(*(*[]byte)(unsafe.Pointer(&input)))
	return
}

