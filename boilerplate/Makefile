obj-m += monitor.o
KDIR := /lib/modules/$(shell uname -r)/build
PWD := $(shell pwd)
WORKLOAD_LDFLAGS ?= -static

USER_TARGETS = engine cpu_hog io_pulse memory_hog

all: $(USER_TARGETS) monitor.ko

ci: $(USER_TARGETS)

engine: engine.c
	gcc -o engine engine.c

cpu_hog: cpu_hog.c
	gcc -o cpu_hog cpu_hog.c $(WORKLOAD_LDFLAGS)

io_pulse: io_pulse.c
	gcc -o io_pulse io_pulse.c $(WORKLOAD_LDFLAGS)

memory_hog: memory_hog.c
	gcc -o memory_hog memory_hog.c $(WORKLOAD_LDFLAGS)

monitor.ko: monitor.c monitor_ioctl.h
	$(MAKE) -C $(KDIR) M=$(PWD) modules

clean:
	if [ -d "$(KDIR)" ]; then $(MAKE) -C $(KDIR) M=$(PWD) clean; fi
	rm -f $(USER_TARGETS) *.o *.mod *.mod.c *.symvers *.order
	rm -f *.log
	rm -rf logs
	rm -f /tmp/mini_runtime.sock

.PHONY: all ci module clean
